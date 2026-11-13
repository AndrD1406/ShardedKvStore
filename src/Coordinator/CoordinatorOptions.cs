using ShardedKvStore.Shared.Hashing;

namespace ShardedKvStore.Coordinator;

/// <summary>
/// Coordinator settings, read from configuration / environment variables:
/// <list type="bullet">
/// <item><c>DATA_DIR</c> – folder for cluster-state.json and the single-node store (default <c>./data</c>; "none" disables files).</item>
/// <item><c>VIRTUAL_NODES</c> – virtual nodes per shard (default 150).</item>
/// <item><c>SHARD_TIMEOUT_SECONDS</c> – per-request timeout towards shards (default 10).</item>
/// </list>
/// </summary>
public sealed record CoordinatorOptions(string? DataDirectory, int VirtualNodes, TimeSpan ShardTimeout)
{
    public string? StateFilePath => DataDirectory is null ? null : Path.Combine(DataDirectory, "cluster-state.json");

    public string? LocalStoreFilePath => DataDirectory is null ? null : Path.Combine(DataDirectory, "coordinator-local-store.json");

    public static CoordinatorOptions FromConfiguration(IConfiguration configuration)
    {
        string dataDir = configuration["DATA_DIR"] ?? "data";
        return new CoordinatorOptions(
            DataDirectory: string.Equals(dataDir, "none", StringComparison.OrdinalIgnoreCase) ? null : dataDir,
            VirtualNodes: configuration.GetValue("VIRTUAL_NODES", ConsistentHashRing<string>.DefaultVirtualNodes),
            ShardTimeout: TimeSpan.FromSeconds(configuration.GetValue("SHARD_TIMEOUT_SECONDS", 10)));
    }
}
