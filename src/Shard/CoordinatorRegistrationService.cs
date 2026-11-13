using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Polly;
using Polly.Retry;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.Shard;

/// <summary>
/// Registers the shard with the coordinator once Kestrel is listening, retrying with exponential
/// back-off (Polly) until the coordinator is reachable, then keeps sending heartbeats (the same
/// idempotent call) so that a restarted coordinator re-learns the shard automatically.
/// </summary>
public sealed class CoordinatorRegistrationService(
    ShardOptions options,
    IHttpClientFactory httpClientFactory,
    IHostApplicationLifetime lifetime,
    IServer server,
    RegistrationState state,
    ILogger<CoordinatorRegistrationService> logger) : BackgroundService
{
    public const string HttpClientName = "coordinator";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.CoordinatorUrl is null)
        {
            logger.LogWarning("COORDINATOR_URL is not set: shard {ShardId} runs standalone and will not register", options.ShardId);
            return;
        }

        if (!await WaitForApplicationStartedAsync(stoppingToken))
        {
            return;
        }

        string selfUrl = options.SelfUrl ?? DetectSelfUrl();
        state.SelfUrl = selfUrl;
        var request = new RegisterShardRequest(options.ShardId, selfUrl);

        ResiliencePipeline registrationPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<TaskCanceledException>(_ => !stoppingToken.IsCancellationRequested),
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Exponential,
                Delay = TimeSpan.FromSeconds(1),
                MaxDelay = TimeSpan.FromSeconds(30),
                UseJitter = true,
                OnRetry = args =>
                {
                    logger.LogWarning("Registration attempt {Attempt} with {Coordinator} failed ({Error}); retrying in {Delay}",
                        args.AttemptNumber + 1, options.CoordinatorUrl, args.Outcome.Exception?.Message, args.RetryDelay);
                    return ValueTask.CompletedTask;
                },
            })
            .Build();

        try
        {
            await registrationPipeline.ExecuteAsync(async ct => await RegisterAsync(request, ct), stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (InvalidOperationException ex)
        {
            logger.LogCritical(ex, "Coordinator rejected shard {ShardId}; giving up", options.ShardId);
            return;
        }

        using var heartbeat = new PeriodicTimer(options.HeartbeatInterval);
        try
        {
            while (await heartbeat.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RegisterAsync(request, stoppingToken);
                }
                catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException)
                {
                    state.Registered = false;
                    logger.LogWarning("Heartbeat to {Coordinator} failed: {Error}", options.CoordinatorUrl, ex.Message);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }

    private async Task RegisterAsync(RegisterShardRequest request, CancellationToken ct)
    {
        HttpClient http = httpClientFactory.CreateClient(HttpClientName);
        var uri = new Uri(new Uri(options.CoordinatorUrl!.TrimEnd('/') + "/"), "api/shards/register");

        using HttpResponseMessage response = await http.PostAsJsonAsync(uri, request, ct);
        if (response.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.BadRequest)
        {
            string body = await response.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException($"HTTP {(int)response.StatusCode}: {body}");
        }

        response.EnsureSuccessStatusCode(); // 5xx -> HttpRequestException -> retried

        RegisterShardResponse? result = await response.Content.ReadFromJsonAsync<RegisterShardResponse>(ct);
        bool wasRegistered = state.Registered;
        state.Registered = true;
        state.LastHeartbeat = DateTimeOffset.UtcNow;

        if (!wasRegistered || result?.IsNew == true)
        {
            logger.LogInformation("Shard {ShardId} registered with {Coordinator} as {SelfUrl} (cluster has {Count} shards)",
                request.ShardId, options.CoordinatorUrl, request.BaseUrl, result?.TotalShards);
        }
    }

    private async Task<bool> WaitForApplicationStartedAsync(CancellationToken stoppingToken)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenRegistration onStarted = lifetime.ApplicationStarted.Register(() => started.TrySetResult());
        using CancellationTokenRegistration onStopping = stoppingToken.Register(() => started.TrySetCanceled());
        try
        {
            await started.Task;
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private string DetectSelfUrl()
    {
        string address = server.Features.Get<IServerAddressesFeature>()?.Addresses.FirstOrDefault()
                         ?? "http://localhost:8080";
        string normalized = address
            .Replace("://+", "://localhost", StringComparison.Ordinal)
            .Replace("://*", "://localhost", StringComparison.Ordinal)
            .Replace("://0.0.0.0", "://localhost", StringComparison.Ordinal)
            .Replace("://[::]", "://localhost", StringComparison.Ordinal);
        logger.LogWarning("SELF_URL is not set; advertising {SelfUrl}. Set SELF_URL when running on several hosts.", normalized);
        return normalized;
    }
}

/// <summary>Registration status shown by the shard's /health endpoint.</summary>
public sealed class RegistrationState
{
    public volatile bool Registered;

    public string? SelfUrl { get; set; }

    public DateTimeOffset? LastHeartbeat { get; set; }
}
