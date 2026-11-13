using Microsoft.AspNetCore.Diagnostics;
using ShardedKvStore.Coordinator.Routing;

namespace ShardedKvStore.Coordinator;

/// <summary>Turns <see cref="ShardUnavailableException"/> into an RFC 7807 503 response.</summary>
public sealed class ShardUnavailableExceptionHandler(
    IProblemDetailsService problemDetails,
    ILogger<ShardUnavailableExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        if (exception is not ShardUnavailableException shardException)
        {
            return false;
        }

        logger.LogWarning(exception, "Request {Method} {Path} failed: shard {ShardId} unavailable",
            httpContext.Request.Method, httpContext.Request.Path, shardException.ShardId);

        httpContext.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        return await problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails =
            {
                Status = StatusCodes.Status503ServiceUnavailable,
                Title = "Shard unavailable",
                Detail = shardException.Message,
                Extensions = { ["shardId"] = shardException.ShardId },
            },
        });
    }
}
