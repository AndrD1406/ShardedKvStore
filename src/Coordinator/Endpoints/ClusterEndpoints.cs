using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Coordinator.Routing;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Hashing;

namespace ShardedKvStore.Coordinator.Endpoints;

public static class ClusterEndpoints
{
    public static IEndpointRouteBuilder MapClusterEndpoints(this IEndpointRouteBuilder app)
    {
        RouteGroupBuilder group = app.MapGroup("/api/cluster").WithTags("Cluster");

        group.MapGet("/stats", GetStats)
            .WithName("GetClusterStats")
            .WithSummary("Cluster statistics")
            .WithDescription("Mode (single-node / sharded), item counts per node and the last key-migration report.");

        group.MapGet("/route", GetRoute)
            .WithName("GetRoute")
            .WithSummary("Show which node owns a partition")
            .WithDescription("Computes hash('tableName:partitionKey') and looks it up on the consistent-hash ring.");

        group.MapPost("/rebalance", TriggerRebalance)
            .WithName("TriggerRebalance")
            .WithSummary("Trigger key migration")
            .WithDescription("Schedules a rebalance pass that moves every partition to its current ring owner. " +
                             "Runs automatically when a shard joins.");

        app.MapGet("/health", () => TypedResults.Ok(new { status = "Healthy", role = "coordinator" }))
            .WithTags("Health")
            .WithName("CoordinatorHealth")
            .WithSummary("Liveness probe");

        return app;
    }

    private static async Task<Ok<ClusterStatsResponse>> GetStats(
        ClusterMetadata metadata, ItemRouter router, RebalanceService rebalancer, CancellationToken ct)
    {
        IReadOnlyList<IItemBackend> backends = router.AllBackends();
        NodeStats[] nodes = await Task.WhenAll(backends.Select(async backend =>
        {
            try
            {
                return new NodeStats(backend.NodeId, true, await backend.GetStatsAsync(ct), null);
            }
            catch (ShardUnavailableException ex)
            {
                return new NodeStats(backend.NodeId, false, null, ex.Message);
            }
        }));

        int shardCount = metadata.ShardCount;
        return TypedResults.Ok(new ClusterStatsResponse(
            Mode: shardCount == 0 ? "single-node" : "sharded",
            Tables: metadata.ListTables().Count,
            Shards: shardCount,
            TotalItems: nodes.Sum(static n => n.Stats?.Items ?? 0),
            Nodes: nodes,
            LastRebalance: rebalancer.LastReport,
            RebalanceInProgress: rebalancer.IsRunning));
    }

    private static Results<Ok<RouteResponse>, BadRequest<ProblemDetails>> GetRoute(
        string tableName, string partitionKey, ClusterMetadata metadata, ItemRouter router)
    {
        if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(partitionKey))
        {
            return TypedResults.BadRequest(Problems.BadRequest("'tableName' and 'partitionKey' are required."));
        }

        string routingKey = ShardRouting.RoutingKey(tableName, partitionKey);
        IItemBackend backend = router.Resolve(tableName, partitionKey);
        return TypedResults.Ok(new RouteResponse(
            tableName,
            partitionKey,
            routingKey,
            MurmurHash3.Hash32(routingKey),
            backend.NodeId,
            metadata.ShardCount == 0 ? "single-node" : "sharded"));
    }

    private static Accepted TriggerRebalance(RebalanceService rebalancer)
    {
        rebalancer.Trigger("manual request");
        return TypedResults.Accepted("/api/cluster/stats");
    }
}
