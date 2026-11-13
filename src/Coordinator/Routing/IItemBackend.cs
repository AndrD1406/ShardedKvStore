using System.Text.Json;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Routing;

/// <summary>
/// A storage node as seen by the coordinator: either its own local store (single-node mode)
/// or a remote shard reached over HTTP.
/// </summary>
public interface IItemBackend
{
    string NodeId { get; }

    Task<bool> TryCreateAsync(PartitionId id, string sortKey, JsonElement item, CancellationToken ct);

    Task<JsonElement?> GetAsync(PartitionId id, string sortKey, CancellationToken ct);

    Task<IReadOnlyList<PartitionEntry>> QueryAsync(PartitionId id, SortKeyRange range, CancellationToken ct);

    Task<bool> ExistsAsync(PartitionId id, string sortKey, CancellationToken ct);

    Task<bool> DeleteAsync(PartitionId id, string sortKey, CancellationToken ct);

    Task<IReadOnlyList<PartitionRef>> ListPartitionsAsync(CancellationToken ct);

    Task<PartitionDump?> ExportPartitionAsync(PartitionId id, CancellationToken ct);

    Task<int> ImportPartitionAsync(PartitionDump dump, CancellationToken ct);

    Task<int> RemoveEntriesAsync(RemoveEntriesRequest request, CancellationToken ct);

    Task<StoreStats> GetStatsAsync(CancellationToken ct);
}
