using Microsoft.AspNetCore.Mvc;

namespace ShardedKvStore.Coordinator.Endpoints;

internal static class Problems
{
    public static ProblemDetails BadRequest(string detail) => Create(StatusCodes.Status400BadRequest, "Bad Request", detail);

    public static ProblemDetails NotFound(string detail) => Create(StatusCodes.Status404NotFound, "Not Found", detail);

    public static ProblemDetails Conflict(string detail) => Create(StatusCodes.Status409Conflict, "Conflict", detail);

    public static ProblemDetails TableNotFound(string tableName) => NotFound($"Table '{tableName}' is not registered.");

    private static ProblemDetails Create(int status, string title, string detail) => new()
    {
        Status = status,
        Title = title,
        Detail = detail,
        Type = $"https://httpstatuses.io/{status}",
    };
}
