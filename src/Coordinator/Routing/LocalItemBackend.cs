using System.Text.Json;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Routing;

/// <summary>The coordinator's own store, used while no shard is registered (single-node mode).</summary>
public sealed class LocalItemBackend(PartitionedItemStore store) : IItemBackend
{
    public const string LocalNodeId = "coordinator-local";

    public string NodeId => LocalNodeId;

    public Task<bool> TryCreateAsync(PartitionId id, string sortKey, JsonElement item, CancellationToken ct) =>
        Task.FromResult(store.TryAdd(id, sortKey, item));

    public Task<JsonElement?> GetAsync(PartitionId id, string sortKey, CancellationToken ct) =>
        Task.FromResult(store.Get(id, sortKey));

    public Task<IReadOnlyList<PartitionEntry>> QueryAsync(PartitionId id, SortKeyRange range, CancellationToken ct) =>
        Task.FromResult(store.Query(id, range));

    public Task<bool> ExistsAsync(PartitionId id, string sortKey, CancellationToken ct) =>
        Task.FromResult(store.Contains(id, sortKey));

    public Task<bool> DeleteAsync(PartitionId id, string sortKey, CancellationToken ct) =>
        Task.FromResult(store.Remove(id, sortKey));

    public Task<IReadOnlyList<PartitionRef>> ListPartitionsAsync(CancellationToken ct) =>
        Task.FromResult(store.ListPartitions());

    public Task<PartitionDump?> ExportPartitionAsync(PartitionId id, CancellationToken ct) =>
        Task.FromResult(store.Export(id));

    public Task<int> ImportPartitionAsync(PartitionDump dump, CancellationToken ct) =>
        Task.FromResult(store.Import(dump));

    public Task<int> RemoveEntriesAsync(RemoveEntriesRequest request, CancellationToken ct) =>
        Task.FromResult(store.RemoveEntries(new PartitionId(request.Table, request.PartitionKey), request.SortKeys));

    public Task<StoreStats> GetStatsAsync(CancellationToken ct) => Task.FromResult(store.GetStats());
}
