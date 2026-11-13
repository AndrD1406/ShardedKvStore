using System.Collections.Concurrent;
using System.Text.Json;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.Shared.Storage;

public readonly record struct PartitionId(string Table, string PartitionKey);

/// <summary>Inclusive sort-key range for partition queries.</summary>
public sealed record SortKeyRange(string? From = null, string? To = null, int? Limit = null)
{
    public static readonly SortKeyRange All = new();
}

/// <summary>
/// Thread-safe in-memory item store.
/// Layout: <c>(table, partitionKey) -&gt; SortedDictionary&lt;sortKey, item&gt;</c> so that a partition
/// is always kept ordered by its sort key (ordinal string comparison). Tables without a sort key
/// store their single item under the empty sort key.
/// </summary>
public sealed class PartitionedItemStore
{
    public const string NoSortKey = "";

    private readonly ConcurrentDictionary<PartitionId, Partition> _partitions = new();
    private long _version;

    /// <summary>Monotonic counter incremented on every mutation (used for dirty tracking).</summary>
    public long Version => Interlocked.Read(ref _version);

    public bool TryAdd(PartitionId id, string sortKey, JsonElement item) =>
        Mutate(id, create: true, partition =>
        {
            if (partition.Items.ContainsKey(sortKey))
            {
                return false;
            }

            partition.Items[sortKey] = item.Clone();
            return true;
        });

    public void Upsert(PartitionId id, string sortKey, JsonElement item) =>
        Mutate(id, create: true, partition =>
        {
            partition.Items[sortKey] = item.Clone();
            return true;
        });

    public JsonElement? Get(PartitionId id, string sortKey) =>
        Read(id, partition => partition.Items.TryGetValue(sortKey, out JsonElement item) ? item : (JsonElement?)null, null);

    public bool Contains(PartitionId id, string sortKey) =>
        Read(id, partition => partition.Items.ContainsKey(sortKey), false);

    public IReadOnlyList<PartitionEntry> Query(PartitionId id, SortKeyRange range) =>
        Read<IReadOnlyList<PartitionEntry>>(id, partition =>
        {
            var result = new List<PartitionEntry>();
            foreach ((string sortKey, JsonElement item) in partition.Items)
            {
                if (range.From is not null && string.CompareOrdinal(sortKey, range.From) < 0)
                {
                    continue;
                }

                if (range.To is not null && string.CompareOrdinal(sortKey, range.To) > 0)
                {
                    break; // items are ordered, nothing further can match
                }

                result.Add(new PartitionEntry(sortKey, item));
                if (range.Limit is { } limit && result.Count >= limit)
                {
                    break;
                }
            }

            return result;
        }, []);

    public bool Remove(PartitionId id, string sortKey) =>
        Mutate(id, create: false, partition => partition.Items.Remove(sortKey));

    public int RemoveEntries(PartitionId id, IEnumerable<string> sortKeys) =>
        MutateCount(id, partition => sortKeys.Count(partition.Items.Remove));

    public IReadOnlyList<PartitionRef> ListPartitions() =>
        [.. _partitions.Keys.Select(static id => new PartitionRef(id.Table, id.PartitionKey))];

    public PartitionDump? Export(PartitionId id)
    {
        IReadOnlyList<PartitionEntry> entries = Query(id, SortKeyRange.All);
        return entries.Count == 0 ? null : new PartitionDump(id.Table, id.PartitionKey, entries);
    }

    /// <summary>
    /// Merges a partition dump. Existing items win unless <paramref name="overwrite"/> is true, so a
    /// write that reached the new owner during migration is never replaced by an older copy.
    /// </summary>
    public int Import(PartitionDump dump, bool overwrite = false)
    {
        var id = new PartitionId(dump.Table, dump.PartitionKey);
        return MutateCount(id, partition =>
        {
            int written = 0;
            foreach (PartitionEntry entry in dump.Entries)
            {
                if (overwrite || !partition.Items.ContainsKey(entry.SortKey))
                {
                    partition.Items[entry.SortKey] = entry.Item.Clone();
                    written++;
                }
            }

            return written;
        }, create: true);
    }

    public StoreStats GetStats()
    {
        var byTable = new Dictionary<string, long>(StringComparer.Ordinal);
        int partitions = 0;
        long items = 0;
        foreach ((PartitionId id, Partition partition) in _partitions)
        {
            int count;
            lock (partition.Gate)
            {
                if (partition.Detached)
                {
                    continue;
                }

                count = partition.Items.Count;
            }

            partitions++;
            items += count;
            byTable[id.Table] = byTable.GetValueOrDefault(id.Table) + count;
        }

        return new StoreStats(partitions, items, byTable);
    }

    public StoreSnapshot CreateSnapshot()
    {
        var dumps = new List<PartitionDump>();
        foreach (PartitionId id in _partitions.Keys)
        {
            if (Export(id) is { } dump)
            {
                dumps.Add(dump);
            }
        }

        return new StoreSnapshot(DateTimeOffset.UtcNow, dumps);
    }

    public void LoadSnapshot(StoreSnapshot snapshot)
    {
        foreach (PartitionDump dump in snapshot.Partitions)
        {
            Import(dump, overwrite: true);
        }
    }

    // -------------------------------------------------------------------------

    private T Read<T>(PartitionId id, Func<Partition, T> read, T missing)
    {
        if (!_partitions.TryGetValue(id, out Partition? partition))
        {
            return missing;
        }

        lock (partition.Gate)
        {
            return partition.Detached ? missing : read(partition);
        }
    }

    private bool Mutate(PartitionId id, bool create, Func<Partition, bool> mutation) =>
        MutateCount(id, partition => mutation(partition) ? 1 : 0, create) > 0;

    private int MutateCount(PartitionId id, Func<Partition, int> mutation, bool create = false)
    {
        while (true)
        {
            Partition? partition;
            if (create)
            {
                partition = _partitions.GetOrAdd(id, static _ => new Partition());
            }
            else if (!_partitions.TryGetValue(id, out partition))
            {
                return 0;
            }

            lock (partition.Gate)
            {
                if (partition.Detached)
                {
                    continue; // removed concurrently; retry against the live instance
                }

                int changed = mutation(partition);
                if (changed > 0)
                {
                    Interlocked.Increment(ref _version);
                }

                if (partition.Items.Count == 0)
                {
                    // Drop empty partitions. Writers holding a stale reference see Detached and retry.
                    partition.Detached = true;
                    _partitions.TryRemove(new KeyValuePair<PartitionId, Partition>(id, partition));
                }

                return changed;
            }
        }
    }

    private sealed class Partition
    {
        public readonly Lock Gate = new();
        public readonly SortedDictionary<string, JsonElement> Items = new(StringComparer.Ordinal);
        public bool Detached;
    }
}
