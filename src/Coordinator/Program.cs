using Microsoft.Extensions.Http.Resilience;
using Microsoft.OpenApi;
using Scalar.AspNetCore;
using ShardedKvStore.Coordinator;
using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Coordinator.Endpoints;
using ShardedKvStore.Coordinator.Routing;
using ShardedKvStore.Shared.Storage;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

CoordinatorOptions options = CoordinatorOptions.FromConfiguration(builder.Configuration);
builder.Services.AddSingleton(options);

// Metadata (tables, shard registry, hash ring) + the local store used in single-node mode.
builder.Services.AddSingleton<ClusterMetadata>();
builder.Services.AddPartitionedItemStore(options.LocalStoreFilePath);
builder.Services.AddSingleton<LocalItemBackend>();
builder.Services.AddSingleton<ItemRouter>();
builder.Services.AddSingleton<RebalanceService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RebalanceService>());

// Coordinator -> shard HTTP: pooled handlers via IHttpClientFactory + Polly-based resilience.
// Retries are disabled for POST/DELETE etc. so a create is never applied twice.
builder.Services.AddHttpClient(ItemRouter.ShardHttpClient)
    .AddStandardResilienceHandler(resilience =>
    {
        resilience.AttemptTimeout.Timeout = options.ShardTimeout;
        resilience.TotalRequestTimeout.Timeout = options.ShardTimeout * 3;
        resilience.CircuitBreaker.SamplingDuration = options.ShardTimeout * 2;
        resilience.Retry.MaxRetryAttempts = 2;
        resilience.Retry.DisableForUnsafeHttpMethods();
    });

builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<ShardUnavailableExceptionHandler>();

builder.Services.AddOpenApi("v1", openApi =>
{
    openApi.AddDocumentTransformer((document, _, _) =>
    {
        document.Info = new OpenApiInfo
        {
            Title = "ShardedKvStore Coordinator API",
            Version = "v1",
            Description = "Distributed key-value store with consistent hashing (150 virtual nodes per shard). " +
                          "Tables have a partition key and an optional sort key; all items of a partition live on one " +
                          "shard, ordered by sort key. With no shard registered the coordinator stores data itself (single-node mode).",
        };
        return Task.CompletedTask;
    });
});

WebApplication app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();

app.MapOpenApi("/openapi/{documentName}.json");
app.MapScalarApiReference(scalar => scalar
    .WithTitle("ShardedKvStore Coordinator")
    .WithOpenApiRoutePattern("/openapi/{documentName}.json"));
app.MapGet("/", () => Results.Redirect("/scalar/v1")).ExcludeFromDescription();

app.MapTableEndpoints();
app.MapShardEndpoints();
app.MapItemEndpoints();
app.MapClusterEndpoints();

app.Run();
