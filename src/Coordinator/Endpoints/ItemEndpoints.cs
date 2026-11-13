using System.ComponentModel;
using System.Text.Json;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Coordinator.Routing;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Endpoints;

public static class ItemEndpoints
{
    public const string StoredOnHeader = "X-Stored-On";
    private const int MaxLimit = 1000;

    public static IEndpointRouteBuilder MapItemEndpoints(this IEndpointRouteBuilder app)
    {
        RouteGroupBuilder group = app.MapGroup("/api/tables/{tableName}/items")
            .WithTags("Items")
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

        group.MapPost("/", CreateItem)
            .WithName("CreateItem")
            .WithSummary("Create an item")
            .WithDescription("Stores an arbitrary JSON object. It must contain the table's partition-key attribute " +
                             "(and sort-key attribute if the table has one) as a string or number. The shard is chosen by " +
                             "hashing 'tableName:partitionKey' on the consistent-hash ring. 409 if the key already exists.");

        group.MapGet("/{partitionKey}", GetItems)
            .WithName("GetItem")
            .WithSummary("Read an item or query a partition")
            .WithDescription("With 'sortKey' (or for tables without a sort key) returns that single item (404 if missing). " +
                             "Without 'sortKey' on a table with a sort key returns all items of the partition ordered by sort key " +
                             "(ordinal), optionally limited to the inclusive range [from, to] and to 'limit' items.");

        group.MapMethods("/{partitionKey}", [HttpMethods.Head], HeadItem)
            .WithName("HeadItem")
            .WithSummary("Check existence (HEAD)")
            .WithDescription("200 if the item exists, 404 otherwise; no body. Without 'sortKey' on a table with a sort key " +
                             "checks whether the partition has any item.");

        group.MapGet("/{partitionKey}/exists", ItemExists)
            .WithName("ItemExists")
            .WithSummary("Check existence (GET, Swagger-friendly)")
            .WithDescription("Same check as HEAD but always answers 200 with a JSON body containing 'exists'.");

        group.MapDelete("/{partitionKey}", DeleteItem)
            .WithName("DeleteItem")
            .WithSummary("Delete an item")
            .WithDescription("Deletes one item. 'sortKey' is required when the table has a sort key. 204 on success, 404 if missing.");

        return app;
    }

    private static async Task<Results<Created<ItemResponse>, BadRequest<ProblemDetails>, NotFound<ProblemDetails>, Conflict<ProblemDetails>>> CreateItem(
        string tableName,
        [FromBody, Description("Arbitrary JSON object containing the key attributes.")] JsonElement item,
        ClusterMetadata metadata,
        ItemRouter router,
        HttpContext http,
        CancellationToken ct)
    {
        if (metadata.GetTable(tableName) is not { } table)
        {
            return TypedResults.NotFound(Problems.TableNotFound(tableName));
        }

        if (!ItemKeys.TryExtract(item, table, out string? partitionKey, out string? sortKey, out string? error))
        {
            return TypedResults.BadRequest(Problems.BadRequest(error));
        }

        IItemBackend backend = router.Resolve(tableName, partitionKey);
        var id = new PartitionId(tableName, partitionKey);
        if (!await backend.TryCreateAsync(id, sortKey, item, ct))
        {
            return TypedResults.Conflict(Problems.Conflict(
                $"Item with key ({DescribeKey(table, partitionKey, sortKey)}) already exists in table '{tableName}'."));
        }

        http.Response.Headers[StoredOnHeader] = backend.NodeId;
        string? responseSortKey = table.SortKey is null ? null : sortKey;
        string location = $"/api/tables/{Uri.EscapeDataString(tableName)}/items/{Uri.EscapeDataString(partitionKey)}" +
                          (responseSortKey is null ? string.Empty : $"?sortKey={Uri.EscapeDataString(responseSortKey)}");

        return TypedResults.Created(location, new ItemResponse(tableName, partitionKey, responseSortKey, backend.NodeId, item));
    }

    private static async Task<Results<Ok<ItemReadResponse>, BadRequest<ProblemDetails>, NotFound<ProblemDetails>>> GetItems(
        string tableName,
        string partitionKey,
        [Description("Sort key of a single item. Omit to query the whole partition.")] string? sortKey,
        [Description("Partition query: inclusive lower bound of the sort key.")] string? from,
        [Description("Partition query: inclusive upper bound of the sort key.")] string? to,
        [Description("Partition query: maximum number of items (1-1000).")] int? limit,
        ClusterMetadata metadata,
        ItemRouter router,
        HttpContext http,
        CancellationToken ct)
    {
        if (metadata.GetTable(tableName) is not { } table)
        {
            return TypedResults.NotFound(Problems.TableNotFound(tableName));
        }

        if (ItemKeys.ValidateKeyValue("partitionKey", partitionKey) is { } pkError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(pkError));
        }

        if (ResolveSortKey(table, sortKey, required: false, out string? effectiveSortKey) is { } skError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(skError));
        }

        IItemBackend backend = router.Resolve(tableName, partitionKey);
        http.Response.Headers[StoredOnHeader] = backend.NodeId;
        var id = new PartitionId(tableName, partitionKey);

        if (effectiveSortKey is not null)
        {
            JsonElement? item = await backend.GetAsync(id, effectiveSortKey, ct);
            if (item is null)
            {
                return TypedResults.NotFound(Problems.NotFound(
                    $"Item ({DescribeKey(table, partitionKey, effectiveSortKey)}) not found in table '{tableName}'."));
            }

            return TypedResults.Ok(new ItemReadResponse(tableName, partitionKey, table.SortKey is null ? null : effectiveSortKey,
                backend.NodeId, 1, [item.Value]));
        }

        if (limit is < 1 or > MaxLimit)
        {
            return TypedResults.BadRequest(Problems.BadRequest($"'limit' must be between 1 and {MaxLimit}."));
        }

        if (from is not null && to is not null && string.CompareOrdinal(from, to) > 0)
        {
            return TypedResults.BadRequest(Problems.BadRequest("'from' must not be greater than 'to'."));
        }

        IReadOnlyList<PartitionEntry> entries = await backend.QueryAsync(id, new SortKeyRange(from, to, limit), ct);
        return TypedResults.Ok(new ItemReadResponse(tableName, partitionKey, null, backend.NodeId, entries.Count,
            [.. entries.Select(static e => e.Item)]));
    }

    private static async Task<Results<Ok, BadRequest, NotFound>> HeadItem(
        string tableName,
        string partitionKey,
        string? sortKey,
        ClusterMetadata metadata,
        ItemRouter router,
        HttpContext http,
        CancellationToken ct)
    {
        if (metadata.GetTable(tableName) is not { } table)
        {
            return TypedResults.NotFound();
        }

        if (ItemKeys.ValidateKeyValue("partitionKey", partitionKey) is not null ||
            ResolveSortKey(table, sortKey, required: false, out string? effectiveSortKey) is not null)
        {
            return TypedResults.BadRequest();
        }

        (bool exists, string node) = await CheckExistsAsync(router, tableName, partitionKey, effectiveSortKey, ct);
        http.Response.Headers[StoredOnHeader] = node;
        return exists ? TypedResults.Ok() : TypedResults.NotFound();
    }

    private static async Task<Results<Ok<ExistsResponse>, BadRequest<ProblemDetails>, NotFound<ProblemDetails>>> ItemExists(
        string tableName,
        string partitionKey,
        [Description("Sort key of the item. Omit to check whether the partition has any item.")] string? sortKey,
        ClusterMetadata metadata,
        ItemRouter router,
        CancellationToken ct)
    {
        if (metadata.GetTable(tableName) is not { } table)
        {
            return TypedResults.NotFound(Problems.TableNotFound(tableName));
        }

        if (ItemKeys.ValidateKeyValue("partitionKey", partitionKey) is { } pkError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(pkError));
        }

        if (ResolveSortKey(table, sortKey, required: false, out string? effectiveSortKey) is { } skError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(skError));
        }

        (bool exists, string node) = await CheckExistsAsync(router, tableName, partitionKey, effectiveSortKey, ct);
        return TypedResults.Ok(new ExistsResponse(tableName, partitionKey,
            table.SortKey is null ? null : effectiveSortKey, exists, node));
    }

    private static async Task<Results<NoContent, BadRequest<ProblemDetails>, NotFound<ProblemDetails>>> DeleteItem(
        string tableName,
        string partitionKey,
        [Description("Sort key of the item; required when the table has a sort key.")] string? sortKey,
        ClusterMetadata metadata,
        ItemRouter router,
        HttpContext http,
        CancellationToken ct)
    {
        if (metadata.GetTable(tableName) is not { } table)
        {
            return TypedResults.NotFound(Problems.TableNotFound(tableName));
        }

        if (ItemKeys.ValidateKeyValue("partitionKey", partitionKey) is { } pkError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(pkError));
        }

        if (ResolveSortKey(table, sortKey, required: true, out string? effectiveSortKey) is { } skError)
        {
            return TypedResults.BadRequest(Problems.BadRequest(skError));
        }

        IItemBackend backend = router.Resolve(tableName, partitionKey);
        http.Response.Headers[StoredOnHeader] = backend.NodeId;
        bool deleted = await backend.DeleteAsync(new PartitionId(tableName, partitionKey), effectiveSortKey!, ct);
        return deleted
            ? TypedResults.NoContent()
            : TypedResults.NotFound(Problems.NotFound(
                $"Item ({DescribeKey(table, partitionKey, effectiveSortKey!)}) not found in table '{tableName}'."));
    }

    // -------------------------------------------------------------------------

    /// <summary>
    /// Maps the optional query-string sort key onto the store's sort key.
    /// Result <paramref name="effectiveSortKey"/> is null only for "whole partition" operations.
    /// </summary>
    private static string? ResolveSortKey(TableDefinition table, string? sortKey, bool required, out string? effectiveSortKey)
    {
        effectiveSortKey = null;
        if (table.SortKey is null)
        {
            if (!string.IsNullOrEmpty(sortKey))
            {
                return $"Table '{table.TableName}' has no sort key; do not pass 'sortKey'.";
            }

            effectiveSortKey = PartitionedItemStore.NoSortKey;
            return null;
        }

        if (string.IsNullOrEmpty(sortKey))
        {
            return required ? $"'sortKey' ({table.SortKey}) is required for table '{table.TableName}'." : null;
        }

        effectiveSortKey = sortKey;
        return ItemKeys.ValidateKeyValue("sortKey", sortKey);
    }

    private static async Task<(bool Exists, string Node)> CheckExistsAsync(
        ItemRouter router, string tableName, string partitionKey, string? sortKey, CancellationToken ct)
    {
        IItemBackend backend = router.Resolve(tableName, partitionKey);
        var id = new PartitionId(tableName, partitionKey);
        bool exists = sortKey is not null
            ? await backend.ExistsAsync(id, sortKey, ct)
            : (await backend.QueryAsync(id, new SortKeyRange(Limit: 1), ct)).Count > 0;
        return (exists, backend.NodeId);
    }

    private static string DescribeKey(TableDefinition table, string partitionKey, string sortKey) =>
        table.SortKey is null
            ? $"{table.PartitionKey}={partitionKey}"
            : $"{table.PartitionKey}={partitionKey}, {table.SortKey}={sortKey}";
}
