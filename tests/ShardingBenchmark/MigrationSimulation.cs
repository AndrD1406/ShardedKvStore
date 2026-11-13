using ShardedKvStore.Shared.Hashing;

namespace ShardedKvStore.ShardingBenchmark;

/// <summary>
/// Offline experiment: distributes keys over 2 shards, adds a 3rd one and measures how many keys
/// change owner with consistent hashing (expected ≈ 1/3) versus naive <c>hash % N</c> (expected ≈ 2/3).
/// </summary>
public static class MigrationSimulation
{
    private static readonly string[] InitialShards = ["shard-1", "shard-2"];
    private const string NewShard = "shard-3";

    public static bool Run(int keyCount, int virtualNodes)
    {
        string[] keys = [.. Enumerable.Range(0, keyCount).Select(i => ShardRouting.RoutingKey("orders", $"user-{i:D6}"))];

        Console.WriteLine($"Ключів: {keyCount:N0}; маршрутизаційний ключ: \"tableName:partitionKey\"; хеш: MurmurHash3 (32 біти)");
        Console.WriteLine($"Віртуальних вузлів на шард: {virtualNodes}");
        Console.WriteLine();

        // ---------------------------------------------------------- consistent hashing
        var ring = new ConsistentHashRing<string>(virtualNodes);
        foreach (string shard in InitialShards)
        {
            ring.Add(shard);
        }

        string[] before = [.. keys.Select(ring.GetNode)];
        IReadOnlyDictionary<string, double> sharesBefore = ring.GetOwnershipShares();

        ring.Add(NewShard);
        string[] after = [.. keys.Select(ring.GetNode)];
        IReadOnlyDictionary<string, double> sharesAfter = ring.GetOwnershipShares();

        int moved = 0, movedElsewhere = 0;
        var movedPerSource = new Dictionary<string, int>();
        for (int i = 0; i < keys.Length; i++)
        {
            if (before[i] == after[i])
            {
                continue;
            }

            moved++;
            movedPerSource[before[i]] = movedPerSource.GetValueOrDefault(before[i]) + 1;
            if (after[i] != NewShard)
            {
                movedElsewhere++;
            }
        }

        string[] allShards = [.. InitialShards, NewShard];
        Dictionary<string, int> countBefore = Count(before);
        Dictionary<string, int> countAfter = Count(after);

        Console.WriteLine("=== Consistent Hashing: розподіл ключів ===");
        Console.WriteLine($"{"Шард",-10} | {"До (2 шарди)",14} | {"% ключів",9} | {"% кільця",9} || {"Після (3 шарди)",15} | {"% ключів",9} | {"% кільця",9} | {"Віддав",7}");
        Console.WriteLine(new string('-', 108));
        foreach (string shard in allShards)
        {
            int b = countBefore.GetValueOrDefault(shard);
            int a = countAfter.GetValueOrDefault(shard);
            Console.WriteLine(
                $"{shard,-10} | {b,14:N0} | {Percent(b, keyCount),8:F2}% | {sharesBefore.GetValueOrDefault(shard) * 100,8:F2}% || " +
                $"{a,15:N0} | {Percent(a, keyCount),8:F2}% | {sharesAfter.GetValueOrDefault(shard) * 100,8:F2}% | {movedPerSource.GetValueOrDefault(shard),7:N0}");
        }

        Console.WriteLine(new string('-', 108));
        Console.WriteLine($"Ідеальний розподіл після додавання: {keyCount / 3.0:N0} ключів (33.33%) на шард; " +
                          $"стандартне відхилення: {StdDevPercent(countAfter, allShards, keyCount):F2} п.п.");
        Console.WriteLine();

        // ---------------------------------------------------------- naive modulo
        uint[] hashes = [.. keys.Select(k => MurmurHash3.Hash32(k))];
        int naiveMoved = hashes.Count(h => h % 2 != h % 3);
        int[] naiveAfter = new int[3];
        foreach (uint h in hashes)
        {
            naiveAfter[h % 3]++;
        }

        double consistentPct = Percent(moved, keyCount);
        double naivePct = Percent(naiveMoved, keyCount);

        Console.WriteLine("=== Міграція ключів при додаванні shard-3 ===");
        Console.WriteLine($"{"Алгоритм",-36} | {"Переїхало",10} | {"Відсоток",9} | {"Теорія",8}");
        Console.WriteLine(new string('-', 74));
        Console.WriteLine($"{"Consistent hashing (" + virtualNodes + " vnodes)",-36} | {moved,10:N0} | {consistentPct,8:F2}% | {"≈33.33%",8}");
        Console.WriteLine($"{"Наївний hash % N (2 → 3)",-36} | {naiveMoved,10:N0} | {naivePct,8:F2}% | {"≈66.67%",8}");
        Console.WriteLine(new string('-', 74));
        Console.WriteLine($"Consistent hashing переносить у {naivePct / Math.Max(consistentPct, 0.0001):F2}× менше ключів, ніж hash % N.");
        Console.WriteLine($"Ключів, що переїхали НЕ на shard-3 (має бути 0): {movedElsewhere}");
        Console.WriteLine($"Наївний розподіл після: shard-1={naiveAfter[0]:N0}, shard-2={naiveAfter[1]:N0}, shard-3={naiveAfter[2]:N0}");
        Console.WriteLine();

        PrintVirtualNodeSensitivity(keys);

        bool ok = movedElsewhere == 0 && consistentPct is > 25 and < 42 && naivePct is > 60 and < 73;
        Console.WriteLine(ok
            ? "РЕЗУЛЬТАТ: OK — переїхало ≈1/3 ключів і лише на новий шард."
            : "РЕЗУЛЬТАТ: FAIL — розподіл/міграція поза очікуваними межами.");
        return ok;
    }

    /// <summary>Shows why virtual nodes matter: balance and migration share for different vnode counts.</summary>
    private static void PrintVirtualNodeSensitivity(string[] keys)
    {
        Console.WriteLine("=== Вплив кількості віртуальних вузлів (3 шарди) ===");
        Console.WriteLine($"{"vnodes",7} | {"min ключів",11} | {"max ключів",11} | {"σ, п.п.",8} | {"переїхало при 2→3",18}");
        Console.WriteLine(new string('-', 66));
        foreach (int vnodes in (int[])[1, 10, 50, 150, 500])
        {
            var ring = new ConsistentHashRing<string>(vnodes);
            ring.Add("shard-1");
            ring.Add("shard-2");
            string[] before = [.. keys.Select(ring.GetNode)];
            ring.Add(NewShard);
            string[] after = [.. keys.Select(ring.GetNode)];

            Dictionary<string, int> counts = Count(after);
            string[] shards = ["shard-1", "shard-2", NewShard];
            int[] values = [.. shards.Select(s => counts.GetValueOrDefault(s))];
            int moved = before.Where((owner, i) => owner != after[i]).Count();

            Console.WriteLine($"{vnodes,7} | {values.Min(),11:N0} | {values.Max(),11:N0} | {StdDevPercent(counts, shards, keys.Length),8:F2} | {Percent(moved, keys.Length),17:F2}%");
        }

        Console.WriteLine();
    }

    private static Dictionary<string, int> Count(IEnumerable<string> owners) =>
        owners.GroupBy(static o => o).ToDictionary(static g => g.Key, static g => g.Count());

    private static double Percent(int part, int total) => total == 0 ? 0 : part * 100.0 / total;

    private static double StdDevPercent(Dictionary<string, int> counts, string[] shards, int total)
    {
        double[] pct = [.. shards.Select(s => Percent(counts.GetValueOrDefault(s), total))];
        double mean = pct.Average();
        return Math.Sqrt(pct.Sum(p => (p - mean) * (p - mean)) / pct.Length);
    }
}
