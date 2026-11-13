using System.ComponentModel;
using System.Text.Json;

namespace ShardedKvStore.Shared.Contracts;

// ---------------------------------------------------------------------------
// Public API (Coordinator)
// ---------------------------------------------------------------------------

[Description("Request to register a new table.")]
public sealed record CreateTableRequest(
    [property: Description("Table name: 3-255 chars, letters, digits, '_', '-', '.'.")] string TableName,
    [property: Description("Name of the item attribute used as partition (hash) key.")] string PartitionKey,
    [property: Description("Optional name of the item attribute used as sort (range) key.")] string? SortKey);

[Description("A registered table definition.")]
public sealed record TableDefinition(
    string TableName,
    string PartitionKey,
    string? SortKey,
    DateTimeOffset CreatedAt);

[Description("Shard self-registration / heartbeat request.")]
public sealed record RegisterShardRequest(
    [property: Description("Unique shard identifier, e.g. shard-1.")] string ShardId,
    [property: Description("Base URL at which the coordinator can reach the shard, e.g. http://10.0.2.10:8080.")] string BaseUrl);

[Description("A shard known to the coordinator.")]
public sealed record ShardInfo(
    string ShardId,
    string BaseUrl,
    DateTimeOffset RegisteredAt,
    DateTimeOffset LastHeartbeat);

[Description("Shard as shown by GET /api/shards, including its share of the hash ring.")]
public sealed record ShardView(
    string ShardId,
    string BaseUrl,
    DateTimeOffset RegisteredAt,
    DateTimeOffset LastHeartbeat,
    int VirtualNodes,
    double RingOwnershipPercent);

[Description("Result of a shard registration.")]
public sealed record RegisterShardResponse(
    ShardInfo Shard,
    [property: Description("True when the shard joined the ring with this call (rebalancing was scheduled).")] bool IsNew,
    int TotalShards);

[Description("A single stored item.")]
public sealed record ItemResponse(
    string TableName,
    string PartitionKey,
    string? SortKey,
    [property: Description("Node that stores the item (shard id or 'coordinator-local' in single-node mode).")] string StoredOn,
    JsonElement Item);

[Description("Result of a read: one item (sortKey given / table without sort key) or a partition range ordered by sort key.")]
public sealed record ItemReadResponse(
    string TableName,
    string PartitionKey,
    [property: Description("Requested sort key; null for a partition (range) query.")] string? SortKey,
    string StoredOn,
    int Count,
    IReadOnlyList<JsonElement> Items);

[Description("Existence check result.")]
public sealed record ExistsResponse(
    string TableName,
    string PartitionKey,
    string? SortKey,
    bool Exists,
    string StoredOn);

[Description("Which node owns a partition.")]
public sealed record RouteResponse(
    string TableName,
    string PartitionKey,
    string RoutingKey,
    uint RoutingHash,
    string Node,
    string Mode);

[Description("Cluster-wide statistics.")]
public sealed record ClusterStatsResponse(
    [property: Description("'single-node' when no shard is registered, otherwise 'sharded'.")] string Mode,
    int Tables,
    int Shards,
    long TotalItems,
    IReadOnlyList<NodeStats> Nodes,
    RebalanceReport? LastRebalance,
    bool RebalanceInProgress);

public sealed record NodeStats(string Node, bool Reachable, StoreStats? Stats, string? Error);

[Description("Outcome of a key-migration (rebalance) run.")]
public sealed record RebalanceReport(
    DateTimeOffset StartedAt,
    DateTimeOffset FinishedAt,
    string Reason,
    int PartitionsScanned,
    int PartitionsMoved,
    int ItemsMoved,
    IReadOnlyList<string> Errors);

// ---------------------------------------------------------------------------
// Internal API (Coordinator -> Shard)
// ---------------------------------------------------------------------------

public sealed record PartitionRef(string Table, string PartitionKey);

public sealed record PartitionEntry(string SortKey, JsonElement Item);

public sealed record PartitionDump(string Table, string PartitionKey, IReadOnlyList<PartitionEntry> Entries);

public sealed record RemoveEntriesRequest(string Table, string PartitionKey, IReadOnlyList<string> SortKeys);

public sealed record CountResponse(int Count);

public sealed record StoreStats(int Partitions, long Items, IReadOnlyDictionary<string, long> ItemsByTable);

public sealed record ShardStatsResponse(string ShardId, StoreStats Stats);

public sealed record ShardHealthResponse(string Status, string ShardId, string? SelfUrl, bool RegisteredWithCoordinator);

public sealed record StoreSnapshot(DateTimeOffset SavedAt, IReadOnlyList<PartitionDump> Partitions);
