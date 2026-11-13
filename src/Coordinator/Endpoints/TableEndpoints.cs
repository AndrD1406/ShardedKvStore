using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using ShardedKvStore.Coordinator.Cluster;
using ShardedKvStore.Shared.Contracts;

namespace ShardedKvStore.Coordinator.Endpoints;

public static class TableEndpoints
{
    public static IEndpointRouteBuilder MapTableEndpoints(this IEndpointRouteBuilder app)
    {
        RouteGroupBuilder group = app.MapGroup("/api/tables").WithTags("Tables");

        group.MapPost("/", CreateTable)
            .WithName("CreateTable")
            .WithSummary("Register a table")
            .WithDescription("Registers table metadata: the partition-key attribute and an optional sort-key attribute. " +
                             "Items written later must contain these attributes.");

        group.MapGet("/", ListTables)
            .WithName("ListTables")
            .WithSummary("List tables")
            .WithDescription("Returns all registered tables ordered by name.");

        group.MapGet("/{tableName}", GetTable)
            .WithName("GetTable")
            .WithSummary("Get a table definition");

        return app;
    }

    private static Results<Created<TableDefinition>, BadRequest<ProblemDetails>, Conflict<ProblemDetails>> CreateTable(
        CreateTableRequest request, ClusterMetadata metadata)
    {
        if (string.IsNullOrWhiteSpace(request.TableName) || !ItemKeys.TableNamePattern().IsMatch(request.TableName))
        {
            return TypedResults.BadRequest(Problems.BadRequest(
                "'tableName' must be 3-255 characters: letters, digits, '_', '-' or '.'."));
        }

        if (string.IsNullOrWhiteSpace(request.PartitionKey) || !ItemKeys.AttributeNamePattern().IsMatch(request.PartitionKey))
        {
            return TypedResults.BadRequest(Problems.BadRequest(
                "'partitionKey' is required: 1-255 characters, letters, digits, '_', '-' or '.'."));
        }

        if (request.SortKey is not null)
        {
            if (!ItemKeys.AttributeNamePattern().IsMatch(request.SortKey))
            {
                return TypedResults.BadRequest(Problems.BadRequest(
                    "'sortKey' must be null or 1-255 characters: letters, digits, '_', '-' or '.'."));
            }

            if (request.SortKey == request.PartitionKey)
            {
                return TypedResults.BadRequest(Problems.BadRequest("'sortKey' must differ from 'partitionKey'."));
            }
        }

        var table = new TableDefinition(request.TableName, request.PartitionKey, request.SortKey, DateTimeOffset.UtcNow);
        if (!metadata.TryAddTable(table))
        {
            return TypedResults.Conflict(Problems.Conflict($"Table '{request.TableName}' already exists."));
        }

        return TypedResults.Created($"/api/tables/{Uri.EscapeDataString(table.TableName)}", table);
    }

    private static Ok<IReadOnlyList<TableDefinition>> ListTables(ClusterMetadata metadata) =>
        TypedResults.Ok(metadata.ListTables());

    private static Results<Ok<TableDefinition>, NotFound<ProblemDetails>> GetTable(string tableName, ClusterMetadata metadata) =>
        metadata.GetTable(tableName) is { } table
            ? TypedResults.Ok(table)
            : TypedResults.NotFound(Problems.TableNotFound(tableName));
}
