using Microsoft.OpenApi;
using Scalar.AspNetCore;
using ShardedKvStore.Shard;
using ShardedKvStore.Shared.Storage;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

ShardOptions options = ShardOptions.FromConfiguration(builder.Configuration);
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<RegistrationState>();

// In-memory ConcurrentDictionary-based store with optional JSON snapshot persistence.
builder.Services.AddPartitionedItemStore(options.PersistencePath);

builder.Services.AddHttpClient(CoordinatorRegistrationService.HttpClientName,
    client => client.Timeout = TimeSpan.FromSeconds(10));
builder.Services.AddHostedService<CoordinatorRegistrationService>();

builder.Services.AddProblemDetails();
builder.Services.AddOpenApi("v1", openApi =>
{
    openApi.AddDocumentTransformer((document, _, _) =>
    {
        document.Info = new OpenApiInfo
        {
            Title = $"ShardedKvStore Shard API ({options.ShardId})",
            Version = "v1",
            Description = "Internal storage-node API used by the coordinator. Clients should talk to the coordinator.",
        };
        return Task.CompletedTask;
    });
});

WebApplication app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();

app.MapOpenApi("/openapi/{documentName}.json");
app.MapScalarApiReference(scalar => scalar
    .WithTitle($"Shard {options.ShardId}")
    .WithOpenApiRoutePattern("/openapi/{documentName}.json"));
app.MapGet("/", () => Results.Redirect("/scalar/v1")).ExcludeFromDescription();

app.MapInternalEndpoints();

app.Logger.LogInformation("Shard {ShardId} starting (coordinator: {Coordinator}, persistence: {Persistence})",
    options.ShardId, options.CoordinatorUrl ?? "<none>", options.PersistencePath ?? "<memory only>");

app.Run();
