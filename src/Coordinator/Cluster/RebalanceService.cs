using System.Threading.Channels;
using ShardedKvStore.Coordinator.Routing;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Cluster;

/// <summary>
/// Moves partitions to their new owner after ring membership changes (a shard joined, or the
/// cluster left single-node mode). For every node it lists the partitions, asks the ring who owns
/// each one now and, if that is another node, copies the partition there (existing items on the
/// target win) and then removes exactly the copied sort keys from the source.
/// <para>
/// Thanks to consistent hashing only ~1/N of the partitions move when the N-th shard joins.
/// Limitations (by design, to keep the demo simple): no tombstones, so an item deleted on its new
/// owner while it is still being copied can reappear; reads of a partition in flight may briefly miss.
/// </para>
/// </summary>
public sealed class RebalanceService(ItemRouter router, ILogger<RebalanceService> logger) : BackgroundService
{
    private static readonly TimeSpan Debounce = TimeSpan.FromSeconds(2);

    // Capacity 1 + DropWrite: a burst of triggers collapses into one pending run.
    private readonly Channel<string> _signals = Channel.CreateBounded<string>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    private volatile RebalanceReport? _lastReport;
    private volatile bool _running;

    public RebalanceReport? LastReport => _lastReport;

    public bool IsRunning => _running;

    public void Trigger(string reason)
    {
        if (_signals.Writer.TryWrite(reason))
        {
            logger.LogInformation("Rebalance scheduled: {Reason}", reason);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // After a restart the local store may hold data that belongs to shards.
        Trigger("coordinator startup");

        await foreach (string reason in _signals.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                await Task.Delay(Debounce, stoppingToken);
                _running = true;
                RebalanceReport report = await RunAsync(reason, stoppingToken);
                _lastReport = report;
                if (report.PartitionsMoved > 0 || report.Errors.Count > 0)
                {
                    logger.LogInformation(
                        "Rebalance '{Reason}' finished: scanned {Scanned}, moved {Moved} partitions / {Items} items, {Errors} errors",
                        reason, report.PartitionsScanned, report.PartitionsMoved, report.ItemsMoved, report.Errors.Count);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Rebalance '{Reason}' failed", reason);
            }
            finally
            {
                _running = false;
            }
        }
    }

    private async Task<RebalanceReport> RunAsync(string reason, CancellationToken ct)
    {
        DateTimeOffset startedAt = DateTimeOffset.UtcNow;
        int scanned = 0, partitionsMoved = 0, itemsMoved = 0;
        var errors = new List<string>();

        foreach (IItemBackend source in router.AllBackends())
        {
            IReadOnlyList<PartitionRef> partitions;
            try
            {
                partitions = await source.ListPartitionsAsync(ct);
            }
            catch (ShardUnavailableException ex)
            {
                errors.Add(ex.Message);
                continue;
            }

            foreach (PartitionRef partition in partitions)
            {
                scanned++;
                IItemBackend target = router.Resolve(partition.Table, partition.PartitionKey);
                if (target.NodeId == source.NodeId)
                {
                    continue;
                }

                try
                {
                    var id = new PartitionId(partition.Table, partition.PartitionKey);
                    PartitionDump? dump = await source.ExportPartitionAsync(id, ct);
                    if (dump is null || dump.Entries.Count == 0)
                    {
                        continue;
                    }

                    await target.ImportPartitionAsync(dump, ct);
                    await source.RemoveEntriesAsync(
                        new RemoveEntriesRequest(dump.Table, dump.PartitionKey, [.. dump.Entries.Select(static e => e.SortKey)]), ct);

                    partitionsMoved++;
                    itemsMoved += dump.Entries.Count;
                    logger.LogDebug("Moved {Table}:{PartitionKey} ({Count} items) {Source} -> {Target}",
                        dump.Table, dump.PartitionKey, dump.Entries.Count, source.NodeId, target.NodeId);
                }
                catch (ShardUnavailableException ex)
                {
                    errors.Add($"{partition.Table}:{partition.PartitionKey}: {ex.Message}");
                }
            }
        }

        return new RebalanceReport(startedAt, DateTimeOffset.UtcNow, reason, scanned, partitionsMoved, itemsMoved, errors);
    }
}
