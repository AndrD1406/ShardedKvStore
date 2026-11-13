using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.Shared.Storage;

/// <param name="FilePath">JSON snapshot file. Null disables persistence (pure in-memory).</param>
/// <param name="FlushInterval">How often a dirty store is flushed to disk.</param>
public sealed record StorePersistenceOptions(string? FilePath, TimeSpan FlushInterval);

/// <summary>
/// Optional durability for <see cref="PartitionedItemStore"/>: loads a JSON snapshot on start,
/// periodically flushes it when the store changed and writes a final snapshot on graceful shutdown.
/// Writes go to a temp file and are atomically moved into place.
/// </summary>
public sealed class StorePersistenceService(
    PartitionedItemStore store,
    StorePersistenceOptions options,
    ILogger<StorePersistenceService> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = false };
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private long _savedVersion = -1;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (options.FilePath is { } path && File.Exists(path))
        {
            try
            {
                await using FileStream stream = File.OpenRead(path);
                StoreSnapshot? snapshot = await JsonSerializer.DeserializeAsync<StoreSnapshot>(stream, JsonOptions, cancellationToken);
                if (snapshot is not null)
                {
                    store.LoadSnapshot(snapshot);
                    logger.LogInformation("Loaded {Partitions} partitions from {Path} (saved at {SavedAt})",
                        snapshot.Partitions.Count, path, snapshot.SavedAt);
                }
            }
            catch (Exception ex) when (ex is JsonException or IOException)
            {
                logger.LogError(ex, "Could not load snapshot {Path}; starting with an empty store", path);
            }
        }

        _savedVersion = store.Version;
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.FilePath is null)
        {
            logger.LogInformation("Persistence disabled: data is kept in memory only");
            return;
        }

        logger.LogInformation("Persisting store to {Path} every {Interval}", options.FilePath, options.FlushInterval);
        using var timer = new PeriodicTimer(options.FlushInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                await FlushAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await FlushAsync(CancellationToken.None);
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (options.FilePath is not { } path)
        {
            return;
        }

        await _flushLock.WaitAsync(cancellationToken);
        try
        {
            long version = store.Version;
            if (version == _savedVersion)
            {
                return;
            }

            StoreSnapshot snapshot = store.CreateSnapshot();
            string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            string tempPath = path + ".tmp";
            await using (FileStream stream = File.Create(tempPath))
            {
                await JsonSerializer.SerializeAsync(stream, snapshot, JsonOptions, cancellationToken);
            }

            File.Move(tempPath, path, overwrite: true);
            _savedVersion = version;
            logger.LogDebug("Snapshot written: {Partitions} partitions -> {Path}", snapshot.Partitions.Count, path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogError(ex, "Failed to write snapshot {Path}", path);
        }
        finally
        {
            _flushLock.Release();
        }
    }
}

public static class StorageServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers a singleton <see cref="PartitionedItemStore"/> and its optional JSON persistence.</summary>
        public IServiceCollection AddPartitionedItemStore(string? persistencePath, TimeSpan? flushInterval = null)
        {
            services.AddSingleton<PartitionedItemStore>();
            services.AddSingleton(new StorePersistenceOptions(
                string.IsNullOrWhiteSpace(persistencePath) ? null : persistencePath,
                flushInterval ?? TimeSpan.FromSeconds(5)));
            services.AddHostedService<StorePersistenceService>();
            return services;
        }
    }
}
