using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.Coordinator.Endpoints;

public static class ShardEndpoints
{
    public static IEndpointRouteBuilder MapShardEndpoints(this IEndpointRouteBuilder app)
    {
        RouteGroupBuilder group = app.MapGroup("/api/shards").WithTags("Shards");

        group.MapPost("/register", RegisterShard)
            .WithName("RegisterShard")
            .WithSummary("Register a shard (or send a heartbeat)")
            .WithDescription("Called by shards on start-up and periodically afterwards. A new shard is added to the " +
                             "consistent-hash ring with its virtual nodes (201) and a key migration is scheduled; a known " +
                             "shard just refreshes its heartbeat / address (200). 409 if the base URL already belongs to another shard.");

        group.MapGet("/", ListShards)
            .WithName("ListShards")
            .WithSummary("List active shards")
            .WithDescription("Registered shards with their virtual-node count and share of the hash ring. " +
                             "An empty list means the coordinator runs in single-node mode.");

        return app;
    }

    private static Results<Created<RegisterShardResponse>, Ok<RegisterShardResponse>, BadRequest<ProblemDetails>, Conflict<ProblemDetails>> RegisterShard(
        RegisterShardRequest request, ClusterMetadata metadata, RebalanceService rebalancer)
    {
        if (string.IsNullOrWhiteSpace(request.ShardId) || !ItemKeys.AttributeNamePattern().IsMatch(request.ShardId))
        {
            return TypedResults.BadRequest(Problems.BadRequest(
                "'shardId' is required: 1-255 characters, letters, digits, '_', '-' or '.'."));
        }

        if (!Uri.TryCreate(request.BaseUrl, UriKind.Absolute, out Uri? baseUri) ||
            (baseUri.Scheme != Uri.UriSchemeHttp && baseUri.Scheme != Uri.UriSchemeHttps))
        {
            return TypedResults.BadRequest(Problems.BadRequest("'baseUrl' must be an absolute http(s) URL."));
        }

        string baseUrl = baseUri.GetLeftPart(UriPartial.Path).TrimEnd('/');
        ShardRegistrationResult result = metadata.RegisterShard(request.ShardId, baseUrl);

        switch (result.Status)
        {
            case ShardRegistrationStatus.AddressConflict:
                return TypedResults.Conflict(Problems.Conflict(
                    $"Base URL '{baseUrl}' is already registered by shard '{result.ConflictingShardId}'."));

            case ShardRegistrationStatus.Joined:
                rebalancer.Trigger($"shard '{request.ShardId}' joined");
                return TypedResults.Created("/api/shards", new RegisterShardResponse(result.Shard!, true, metadata.ShardCount));

            default:
                return TypedResults.Ok(new RegisterShardResponse(result.Shard!, false, metadata.ShardCount));
        }
    }

    private static Ok<IReadOnlyList<ShardView>> ListShards(ClusterMetadata metadata)
    {
        IReadOnlyDictionary<string, double> ownership = metadata.GetRingOwnership();
        IReadOnlyList<ShardView> shards =
        [
            .. metadata.ListShards().Select(s => new ShardView(
                s.ShardId,
                s.BaseUrl,
                s.RegisteredAt,
                s.LastHeartbeat,
                metadata.VirtualNodesPerShard,
                Math.Round(ownership.GetValueOrDefault(s.ShardId) * 100, 2))),
        ];
        return TypedResults.Ok(shards);
    }
}
