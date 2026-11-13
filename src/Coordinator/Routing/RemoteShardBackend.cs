using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Routing;

/// <summary>Thrown when a shard cannot be reached or answers unexpectedly; mapped to HTTP 503.</summary>
public sealed class ShardUnavailableException(string shardId, string message, Exception? inner = null)
    : Exception($"Shard '{shardId}' is unavailable: {message}", inner)
{
    public string ShardId { get; } = shardId;
}

/// <summary>HTTP client for a shard's internal API (<c>/internal/*</c>).</summary>
public sealed class RemoteShardBackend(HttpClient http, ShardInfo shard) : IItemBackend
{
    public string NodeId => shard.ShardId;

    public async Task<bool> TryCreateAsync(PartitionId id, string sortKey, JsonElement item, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Post, ItemUrl("internal/items", id, sortKey), JsonContent.Create(item), ct);
        return response.StatusCode switch
        {
            HttpStatusCode.Created or HttpStatusCode.OK => true,
            HttpStatusCode.Conflict => false,
            _ => throw Unexpected(response),
        };
    }

    public async Task<JsonElement?> GetAsync(PartitionId id, string sortKey, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Get, ItemUrl("internal/items", id, sortKey), null, ct);
        return response.StatusCode switch
        {
            HttpStatusCode.OK => await response.Content.ReadFromJsonAsync<JsonElement>(ct),
            HttpStatusCode.NotFound => null,
            _ => throw Unexpected(response),
        };
    }

    public async Task<IReadOnlyList<PartitionEntry>> QueryAsync(PartitionId id, SortKeyRange range, CancellationToken ct)
    {
        var url = new StringBuilder(ItemUrl("internal/items/entries", id, null));
        AppendQuery(url, "from", range.From);
        AppendQuery(url, "to", range.To);
        AppendQuery(url, "limit", range.Limit?.ToString(System.Globalization.CultureInfo.InvariantCulture));

        using HttpResponseMessage response = await SendAsync(HttpMethod.Get, url.ToString(), null, ct);
        EnsureSuccess(response);
        return await response.Content.ReadFromJsonAsync<List<PartitionEntry>>(ct) ?? [];
    }

    public async Task<bool> ExistsAsync(PartitionId id, string sortKey, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Head, ItemUrl("internal/items", id, sortKey), null, ct);
        return response.StatusCode switch
        {
            HttpStatusCode.OK => true,
            HttpStatusCode.NotFound => false,
            _ => throw Unexpected(response),
        };
    }

    public async Task<bool> DeleteAsync(PartitionId id, string sortKey, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Delete, ItemUrl("internal/items", id, sortKey), null, ct);
        return response.StatusCode switch
        {
            HttpStatusCode.NoContent or HttpStatusCode.OK => true,
            HttpStatusCode.NotFound => false,
            _ => throw Unexpected(response),
        };
    }

    public async Task<IReadOnlyList<PartitionRef>> ListPartitionsAsync(CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Get, "internal/partitions", null, ct);
        EnsureSuccess(response);
        return await response.Content.ReadFromJsonAsync<List<PartitionRef>>(ct) ?? [];
    }

    public async Task<PartitionDump?> ExportPartitionAsync(PartitionId id, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Get, ItemUrl("internal/partitions/dump", id, null), null, ct);
        return response.StatusCode switch
        {
            HttpStatusCode.OK => await response.Content.ReadFromJsonAsync<PartitionDump>(ct),
            HttpStatusCode.NotFound => null,
            _ => throw Unexpected(response),
        };
    }

    public async Task<int> ImportPartitionAsync(PartitionDump dump, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Post, "internal/partitions/import", JsonContent.Create(dump), ct);
        EnsureSuccess(response);
        return (await response.Content.ReadFromJsonAsync<CountResponse>(ct))?.Count ?? 0;
    }

    public async Task<int> RemoveEntriesAsync(RemoveEntriesRequest request, CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Post, "internal/partitions/remove", JsonContent.Create(request), ct);
        EnsureSuccess(response);
        return (await response.Content.ReadFromJsonAsync<CountResponse>(ct))?.Count ?? 0;
    }

    public async Task<StoreStats> GetStatsAsync(CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(HttpMethod.Get, "internal/stats", null, ct);
        EnsureSuccess(response);
        ShardStatsResponse? stats = await response.Content.ReadFromJsonAsync<ShardStatsResponse>(ct);
        return stats?.Stats ?? throw new ShardUnavailableException(shard.ShardId, "empty stats response");
    }

    // -------------------------------------------------------------------------

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string relativeUrl, HttpContent? content, CancellationToken ct)
    {
        var request = new HttpRequestMessage(method, new Uri(new Uri(shard.BaseUrl.TrimEnd('/') + "/"), relativeUrl))
        {
            Content = content,
        };

        try
        {
            return await http.SendAsync(request, ct);
        }
        catch (HttpRequestException ex)
        {
            throw new ShardUnavailableException(shard.ShardId, ex.Message, ex);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new ShardUnavailableException(shard.ShardId, "request timed out", ex);
        }
        catch (Polly.Timeout.TimeoutRejectedException ex)
        {
            throw new ShardUnavailableException(shard.ShardId, "request timed out", ex);
        }
        catch (Polly.CircuitBreaker.BrokenCircuitException ex)
        {
            throw new ShardUnavailableException(shard.ShardId, "circuit breaker is open", ex);
        }
    }

    private static string ItemUrl(string path, PartitionId id, string? sortKey)
    {
        var url = new StringBuilder(path)
            .Append("?table=").Append(Uri.EscapeDataString(id.Table))
            .Append("&partitionKey=").Append(Uri.EscapeDataString(id.PartitionKey));
        if (!string.IsNullOrEmpty(sortKey))
        {
            url.Append("&sortKey=").Append(Uri.EscapeDataString(sortKey));
        }

        return url.ToString();
    }

    private static void AppendQuery(StringBuilder url, string name, string? value)
    {
        if (value is not null)
        {
            url.Append('&').Append(name).Append('=').Append(Uri.EscapeDataString(value));
        }
    }

    private void EnsureSuccess(HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            throw Unexpected(response);
        }
    }

    private ShardUnavailableException Unexpected(HttpResponseMessage response) =>
        new(shard.ShardId, $"unexpected HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
}
