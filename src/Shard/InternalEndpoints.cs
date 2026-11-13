using System.ComponentModel;
using System.Text.Json;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Shard;

/// <summary>
/// Internal API called by the coordinator. The shard is schema-less: the coordinator validates the
/// item against the table definition and passes the extracted keys explicitly. For tables without a
/// sort key the <c>sortKey</c> parameter is omitted (stored under the empty sort key).
/// </summary>
public static class InternalEndpoints
{
    public static IEndpointRouteBuilder MapInternalEndpoints(this IEndpointRouteBuilder app)
    {
        RouteGroupBuilder items = app.MapGroup("/internal/items").WithTags("Internal: Items");

        items.MapPost("/", Create)
            .WithName("ShardCreateItem")
            .WithSummary("Store an item if its key is free (201) or report a conflict (409)");

        items.MapGet("/", Get)
            .WithName("ShardGetItem")
            .WithSummary("Return the raw item JSON");

        items.MapMethods("/", [HttpMethods.Head], Head)
            .WithName("ShardHeadItem")
            .WithSummary("200 if the item exists, otherwise 404");

        items.MapGet("/exists", Exists)
            .WithName("ShardItemExists")
            .WithSummary("Existence check with a JSON body");

        items.MapDelete("/", Delete)
            .WithName("ShardDeleteItem")
            .WithSummary("Delete an item");

        items.MapGet("/entries", Query)
            .WithName("ShardQueryPartition")
            .WithSummary("Items of a partition ordered by sort key, optionally within [from, to] and limited");

        RouteGroupBuilder partitions = app.MapGroup("/internal/partitions").WithTags("Internal: Partitions (migration)");

        partitions.MapGet("/", (PartitionedItemStore store) => TypedResults.Ok(store.ListPartitions()))
            .WithName("ShardListPartitions")
            .WithSummary("All partitions stored on this shard");

        partitions.MapGet("/dump", Dump)
            .WithName("ShardDumpPartition")
            .WithSummary("Export a whole partition");

        partitions.MapPost("/import", (PartitionDump dump, PartitionedItemStore store) =>
                TypedResults.Ok(new CountResponse(store.Import(dump))))
            .WithName("ShardImportPartition")
            .WithSummary("Merge a partition dump (existing items win)");

        partitions.MapPost("/remove", (RemoveEntriesRequest request, PartitionedItemStore store) =>
                TypedResults.Ok(new CountResponse(store.RemoveEntries(new PartitionId(request.Table, request.PartitionKey), request.SortKeys))))
            .WithName("ShardRemoveEntries")
            .WithSummary("Remove the given sort keys of a partition (after migration)");

        app.MapGet("/internal/stats", (PartitionedItemStore store, ShardOptions options) =>
                TypedResults.Ok(new ShardStatsResponse(options.ShardId, store.GetStats())))
            .WithTags("Internal: Stats")
            .WithName("ShardStats")
            .WithSummary("Partition and item counts");

        app.MapGet("/health", (ShardOptions options, RegistrationState state) =>
                TypedResults.Ok(new ShardHealthResponse("Healthy", options.ShardId, state.SelfUrl, state.Registered)))
            .WithTags("Health")
            .WithName("ShardHealth")
            .WithSummary("Liveness probe and registration status");

        return app;
    }

    private static Results<Created, Conflict, BadRequest<string>> Create(
        [Description("Table name")] string table,
        [Description("Partition key value")] string partitionKey,
        [Description("Sort key value; omit for tables without a sort key")] string? sortKey,
        [FromBody] JsonElement item,
        PartitionedItemStore store)
    {
        if (item.ValueKind != JsonValueKind.Object)
        {
            return TypedResults.BadRequest("Item must be a JSON object.");
        }

        return store.TryAdd(new PartitionId(table, partitionKey), sortKey ?? PartitionedItemStore.NoSortKey, item)
            ? TypedResults.Created()
            : TypedResults.Conflict();
    }

    private static Results<Ok<JsonElement>, NotFound> Get(string table, string partitionKey, string? sortKey, PartitionedItemStore store) =>
        store.Get(new PartitionId(table, partitionKey), sortKey ?? PartitionedItemStore.NoSortKey) is { } item
            ? TypedResults.Ok(item)
            : TypedResults.NotFound();

    private static Results<Ok, NotFound> Head(string table, string partitionKey, string? sortKey, PartitionedItemStore store) =>
        store.Contains(new PartitionId(table, partitionKey), sortKey ?? PartitionedItemStore.NoSortKey)
            ? TypedResults.Ok()
            : TypedResults.NotFound();

    private static Ok<bool> Exists(string table, string partitionKey, string? sortKey, PartitionedItemStore store) =>
        TypedResults.Ok(store.Contains(new PartitionId(table, partitionKey), sortKey ?? PartitionedItemStore.NoSortKey));

    private static Results<NoContent, NotFound> Delete(string table, string partitionKey, string? sortKey, PartitionedItemStore store) =>
        store.Remove(new PartitionId(table, partitionKey), sortKey ?? PartitionedItemStore.NoSortKey)
            ? TypedResults.NoContent()
            : TypedResults.NotFound();

    private static Ok<IReadOnlyList<PartitionEntry>> Query(
        string table, string partitionKey, string? from, string? to, int? limit, PartitionedItemStore store) =>
        TypedResults.Ok(store.Query(new PartitionId(table, partitionKey), new SortKeyRange(from, to, limit)));

    private static Results<Ok<PartitionDump>, NotFound> Dump(string table, string partitionKey, PartitionedItemStore store) =>
        store.Export(new PartitionId(table, partitionKey)) is { } dump
            ? TypedResults.Ok(dump)
            : TypedResults.NotFound();
}
