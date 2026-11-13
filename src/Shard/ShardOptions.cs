namespace ShardedKvStore.Shard;

/// <summary>
/// Shard settings from environment variables / configuration:
/// <list type="bullet">
/// <item><c>SHARD_ID</c> – unique id (default <c>shard-local</c>).</item>
/// <item><c>SELF_URL</c> – URL the coordinator uses to reach this shard. If omitted, the first Kestrel address is used.</item>
/// <item><c>COORDINATOR_URL</c> – coordinator base URL. If omitted the shard runs standalone (no registration).</item>
/// <item><c>PERSISTENCE_PATH</c> – JSON snapshot file (default <c>data/{SHARD_ID}.json</c>; "none" = memory only).</item>
/// <item><c>HEARTBEAT_SECONDS</c> – re-registration interval (default 15).</item>
/// </list>
/// </summary>
public sealed record ShardOptions(
    string ShardId,
    string? SelfUrl,
    string? CoordinatorUrl,
    string? PersistencePath,
    TimeSpan HeartbeatInterval)
{
    public static ShardOptions FromConfiguration(IConfiguration configuration)
    {
        string shardId = configuration["SHARD_ID"] ?? "shard-local";
        string persistence = configuration["PERSISTENCE_PATH"] ?? Path.Combine("data", $"{shardId}.json");

        return new ShardOptions(
            ShardId: shardId,
            SelfUrl: NullIfEmpty(configuration["SELF_URL"]),
            CoordinatorUrl: NullIfEmpty(configuration["COORDINATOR_URL"]),
            PersistencePath: string.Equals(persistence, "none", StringComparison.OrdinalIgnoreCase) ? null : persistence,
            HeartbeatInterval: TimeSpan.FromSeconds(configuration.GetValue("HEARTBEAT_SECONDS", 15)));
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
