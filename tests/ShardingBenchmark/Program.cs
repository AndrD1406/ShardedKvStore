using System.Text;
using ShardedKvStore.ShardingBenchmark;
using ShardedKvStore.Shared.Hashing;

Console.OutputEncoding = Encoding.UTF8;

// Usage:
//   dotnet run --project tests/ShardingBenchmark                                   offline simulation (10 000 keys)
//   dotnet run --project tests/ShardingBenchmark -- --keys 100000 --vnodes 150
//   dotnet run --project tests/ShardingBenchmark -- --cluster http://localhost:5000 [--items 2000] [--verify-only]

int keys = GetInt("--keys", 10_000);
int vnodes = GetInt("--vnodes", ConsistentHashRing<string>.DefaultVirtualNodes);
int items = GetInt("--items", 2_000);
string? cluster = GetString("--cluster");
bool verifyOnly = args.Contains("--verify-only");

bool ok = cluster is null
    ? MigrationSimulation.Run(keys, vnodes)
    : await LiveClusterCheck.RunAsync(cluster, items, verifyOnly);

return ok ? 0 : 1;

string? GetString(string name)
{
    int index = Array.IndexOf(args, name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
}

int GetInt(string name, int fallback) =>
    int.TryParse(GetString(name), out int value) && value > 0 ? value : fallback;
