using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Hashing;

namespace ShardedKvStore.Coordinator.Routing;

/// <summary>
/// Picks the storage node for a partition. With no shard registered everything stays on the
/// coordinator (single-node mode); otherwise the consistent-hash ring decides, using
/// <c>tableName:partitionKey</c> as the routing key.
/// </summary>
public sealed class ItemRouter(ClusterMetadata metadata, LocalItemBackend local, IHttpClientFactory httpClientFactory)
{
    public const string ShardHttpClient = "shards";

    public LocalItemBackend Local => local;

    public IItemBackend Resolve(string tableName, string partitionKey) =>
        metadata.TryResolveShard(ShardRouting.RoutingKey(tableName, partitionKey), out ShardInfo? shard)
            ? ForShard(shard)
            : local;

    public IItemBackend ForShard(ShardInfo shard) =>
        new RemoteShardBackend(httpClientFactory.CreateClient(ShardHttpClient), shard);

    /// <summary>The local store plus every registered shard.</summary>
    public IReadOnlyList<IItemBackend> AllBackends() =>
        [local, .. metadata.ListShards().Select(ForShard)];
}
