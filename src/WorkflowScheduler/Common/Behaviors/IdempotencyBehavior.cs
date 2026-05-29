using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Common.Behaviors;

public class IdempotencyBehavior<TRequest, TResponse>(IIdempotencyCache idempotencyCache)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken ct)
    {
        if (request is not IIdempotentCommand cmd) return await next(ct);

        if (await idempotencyCache.ExistsAsync(cmd.IdempotencyKey, ct)) return default!;

        var response = await next(ct);
        await idempotencyCache.SaveAsync(cmd.IdempotencyKey, ct);
        return response;
    }
}