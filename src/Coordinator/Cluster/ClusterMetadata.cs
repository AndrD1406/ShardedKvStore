using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Hashing;

namespace ShardedKvStore.Coordinator.Cluster;

public enum ShardRegistrationStatus
{
    Joined,
    Heartbeat,
    AddressUpdated,
    AddressConflict,
}

public sealed record ShardRegistrationResult(ShardRegistrationStatus Status, ShardInfo? Shard, string? ConflictingShardId = null);

/// <summary>
/// Coordinator metadata: table definitions, shard registry and the consistent-hash ring.
/// The state is persisted to a small JSON file so that a coordinator restart keeps tables and
/// ring membership (shards also re-register via heartbeats).
/// </summary>
public sealed class ClusterMetadata
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    private readonly Lock _gate = new();
    private readonly Dictionary<string, TableDefinition> _tables = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ShardInfo> _shards = new(StringComparer.Ordinal);
    private readonly ConsistentHashRing<string> _ring;
    private readonly string? _stateFile;
    private readonly ILogger<ClusterMetadata> _logger;

    public ClusterMetadata(CoordinatorOptions options, ILogger<ClusterMetadata> logger)
    {
        _logger = logger;
        _ring = new ConsistentHashRing<string>(options.VirtualNodes);
        _stateFile = options.StateFilePath;
        Load();
    }

    public int VirtualNodesPerShard => _ring.VirtualNodesPerNode;

    public int ShardCount
    {
        get
        {
            lock (_gate)
            {
                return _shards.Count;
            }
        }
    }

    // ---------------------------------------------------------------- tables

    public bool TryAddTable(TableDefinition table)
    {
        lock (_gate)
        {
            if (!_tables.TryAdd(table.TableName, table))
            {
                return false;
            }

            Save();
            return true;
        }
    }

    public TableDefinition? GetTable(string tableName)
    {
        lock (_gate)
        {
            return _tables.GetValueOrDefault(tableName);
        }
    }

    public IReadOnlyList<TableDefinition> ListTables()
    {
        lock (_gate)
        {
            return [.. _tables.Values.OrderBy(static t => t.TableName, StringComparer.Ordinal)];
        }
    }

    // ---------------------------------------------------------------- shards

    public ShardRegistrationResult RegisterShard(string shardId, string baseUrl)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        lock (_gate)
        {
            ShardInfo? owner = _shards.Values.FirstOrDefault(s =>
                s.ShardId != shardId && string.Equals(s.BaseUrl, baseUrl, StringComparison.OrdinalIgnoreCase));
            if (owner is not null)
            {
                return new ShardRegistrationResult(ShardRegistrationStatus.AddressConflict, null, owner.ShardId);
            }

            if (_shards.TryGetValue(shardId, out ShardInfo? existing))
            {
                bool moved = !string.Equals(existing.BaseUrl, baseUrl, StringComparison.OrdinalIgnoreCase);
                ShardInfo updated = existing with { BaseUrl = baseUrl, LastHeartbeat = now };
                _shards[shardId] = updated;
                if (moved)
                {
                    Save();
                    _logger.LogInformation("Shard {ShardId} moved to {BaseUrl}", shardId, baseUrl);
                }

                return new ShardRegistrationResult(
                    moved ? ShardRegistrationStatus.AddressUpdated : ShardRegistrationStatus.Heartbeat, updated);
            }

            var shard = new ShardInfo(shardId, baseUrl, now, now);
            _shards[shardId] = shard;
            _ring.Add(shardId);
            Save();
            _logger.LogInformation("Shard {ShardId} joined the ring at {BaseUrl} ({Count} shards, {VNodes} vnodes)",
                shardId, baseUrl, _shards.Count, _ring.VirtualNodeCount);
            return new ShardRegistrationResult(ShardRegistrationStatus.Joined, shard);
        }
    }

    public IReadOnlyList<ShardInfo> ListShards()
    {
        lock (_gate)
        {
            return [.. _shards.Values.OrderBy(static s => s.ShardId, StringComparer.Ordinal)];
        }
    }

    public IReadOnlyDictionary<string, double> GetRingOwnership() => _ring.GetOwnershipShares();

    /// <summary>Finds the shard owning <paramref name="routingKey"/>; false in single-node mode.</summary>
    public bool TryResolveShard(string routingKey, [NotNullWhen(true)] out ShardInfo? shard)
    {
        if (_ring.TryGetNode(routingKey, out string? shardId))
        {
            lock (_gate)
            {
                return _shards.TryGetValue(shardId, out shard);
            }
        }

        shard = null;
        return false;
    }

    // ---------------------------------------------------------------- persistence

    private void Load()
    {
        if (_stateFile is null || !File.Exists(_stateFile))
        {
            return;
        }

        try
        {
            PersistedState? state = JsonSerializer.Deserialize<PersistedState>(File.ReadAllText(_stateFile), JsonOptions);
            if (state is null)
            {
                return;
            }

            foreach (TableDefinition table in state.Tables)
            {
                _tables[table.TableName] = table;
            }

            foreach (ShardInfo shard in state.Shards)
            {
                _shards[shard.ShardId] = shard;
                _ring.Add(shard.ShardId);
            }

            _logger.LogInformation("Restored cluster state: {Tables} tables, {Shards} shards", _tables.Count, _shards.Count);
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            _logger.LogError(ex, "Could not read cluster state {Path}; starting empty", _stateFile);
        }
    }

    /// <summary>Must be called while holding <see cref="_gate"/>.</summary>
    private void Save()
    {
        if (_stateFile is null)
        {
            return;
        }

        try
        {
            string? directory = Path.GetDirectoryName(Path.GetFullPath(_stateFile));
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var state = new PersistedState([.. _tables.Values], [.. _shards.Values]);
            string tempPath = _stateFile + ".tmp";
            File.WriteAllText(tempPath, JsonSerializer.Serialize(state, JsonOptions));
            File.Move(tempPath, _stateFile, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogError(ex, "Could not persist cluster state to {Path}", _stateFile);
        }
    }

    private sealed record PersistedState(List<TableDefinition> Tables, List<ShardInfo> Shards);
}
