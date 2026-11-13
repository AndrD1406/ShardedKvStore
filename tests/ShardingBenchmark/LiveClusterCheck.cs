using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.ShardingBenchmark;

/// <summary>
/// End-to-end check against a running cluster: registers a table, writes items through the
/// coordinator, reads every one of them back, checks a sorted partition query and prints how the
/// items are spread across nodes. Run it with <c>--verify-only</c> after adding a shard to prove
/// that migrated items are still readable.
/// </summary>
public static class LiveClusterCheck
{
    private const string Table = "bench_orders";
    private const int Partitions = 250;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static async Task<bool> RunAsync(string coordinatorUrl, int itemCount, bool verifyOnly)
    {
        using var http = new HttpClient { BaseAddress = new Uri(coordinatorUrl.TrimEnd('/') + "/"), Timeout = TimeSpan.FromSeconds(30) };

        Console.WriteLine($"Координатор: {http.BaseAddress}");
        List<ShardView> shards = await http.GetFromJsonAsync<List<ShardView>>("api/shards", Json) ?? [];
        Console.WriteLine(shards.Count == 0
            ? "Шардів немає — координатор працює в режимі Single-Node."
            : $"Активні шарди: {string.Join(", ", shards.Select(s => $"{s.ShardId} ({s.RingOwnershipPercent:F1}% кільця)"))}");

        using (HttpResponseMessage create = await http.PostAsJsonAsync("api/tables",
                   new CreateTableRequest(Table, "userId", "orderId"), Json))
        {
            if (create.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.Conflict))
            {
                Console.WriteLine($"Не вдалося створити таблицю: HTTP {(int)create.StatusCode}");
                return false;
            }
        }

        if (!verifyOnly)
        {
            var sw = Stopwatch.StartNew();
            int created = 0, existed = 0, failed = 0;
            await Parallel.ForEachAsync(Enumerable.Range(0, itemCount), new ParallelOptions { MaxDegreeOfParallelism = 32 },
                async (i, ct) =>
                {
                    var item = new
                    {
                        userId = UserId(i),
                        orderId = OrderId(i),
                        amount = Math.Round(10 + (i % 97) * 1.37, 2),
                        status = i % 3 == 0 ? "shipped" : "new",
                    };
                    using HttpResponseMessage response = await http.PostAsJsonAsync($"api/tables/{Table}/items", item, Json, ct);
                    switch (response.StatusCode)
                    {
                        case HttpStatusCode.Created: Interlocked.Increment(ref created); break;
                        case HttpStatusCode.Conflict: Interlocked.Increment(ref existed); break;
                        default: Interlocked.Increment(ref failed); break;
                    }
                });
            Console.WriteLine($"Запис {itemCount:N0} елементів: створено {created:N0}, вже існували {existed:N0}, помилок {failed:N0} " +
                              $"за {sw.Elapsed.TotalSeconds:F2} с ({itemCount / sw.Elapsed.TotalSeconds:N0} оп/с)");
            if (failed > 0)
            {
                return false;
            }
        }

        // Read every item back by its full key.
        var readWatch = Stopwatch.StartNew();
        int found = 0, missing = 0;
        var perNode = new Dictionary<string, int>();
        var gate = new Lock();
        await Parallel.ForEachAsync(Enumerable.Range(0, itemCount), new ParallelOptions { MaxDegreeOfParallelism = 32 },
            async (i, ct) =>
            {
                using HttpResponseMessage response = await http.GetAsync(
                    $"api/tables/{Table}/items/{UserId(i)}?sortKey={OrderId(i)}", ct);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Interlocked.Increment(ref found);
                    string node = response.Headers.TryGetValues("X-Stored-On", out IEnumerable<string>? values) ? values.First() : "?";
                    lock (gate)
                    {
                        perNode[node] = perNode.GetValueOrDefault(node) + 1;
                    }
                }
                else
                {
                    Interlocked.Increment(ref missing);
                }
            });
        Console.WriteLine($"Читання: знайдено {found:N0}/{itemCount:N0}, відсутні {missing:N0} за {readWatch.Elapsed.TotalSeconds:F2} с");
        foreach ((string node, int count) in perNode.OrderBy(static p => p.Key))
        {
            Console.WriteLine($"  {node,-18} {count,7:N0} елементів ({count * 100.0 / Math.Max(found, 1):F1}%)");
        }

        // Range query inside one partition must be sorted by sort key.
        ItemReadResponse? partition = await http.GetFromJsonAsync<ItemReadResponse>($"api/tables/{Table}/items/{UserId(0)}", Json);
        string[] orderIds = [.. partition?.Items.Select(static i => i.GetProperty("orderId").GetString()!) ?? []];
        bool sorted = orderIds.SequenceEqual(orderIds.Order(StringComparer.Ordinal));
        int expectedInPartition = Enumerable.Range(0, itemCount).Count(i => i % Partitions == 0);
        Console.WriteLine($"Range query партиції {UserId(0)}: {orderIds.Length} елементів (очікувалось {expectedInPartition}), " +
                          $"відсортовано за sortKey: {(sorted ? "так" : "НІ")}, вузол: {partition?.StoredOn}");

        ClusterStatsResponse? stats = await http.GetFromJsonAsync<ClusterStatsResponse>("api/cluster/stats", Json);
        if (stats is not null)
        {
            Console.WriteLine($"Кластер: режим {stats.Mode}, шардів {stats.Shards}, усього елементів {stats.TotalItems:N0}, " +
                              $"міграція триває: {stats.RebalanceInProgress}");
            if (stats.LastRebalance is { } r)
            {
                Console.WriteLine($"  Остання міграція ({r.Reason}): переглянуто {r.PartitionsScanned} партицій, " +
                                  $"перенесено {r.PartitionsMoved} партицій / {r.ItemsMoved} елементів");
            }
        }

        bool ok = missing == 0 && sorted && orderIds.Length == expectedInPartition;
        Console.WriteLine(ok ? "РЕЗУЛЬТАТ: OK" : "РЕЗУЛЬТАТ: FAIL");
        return ok;
    }

    private static string UserId(int i) => $"user-{i % Partitions:D4}";

    private static string OrderId(int i) => $"order-{i:D6}";
}
