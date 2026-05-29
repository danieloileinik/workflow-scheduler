using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Common.Behaviors;

public class TransactionBehavior<TRequest, TResponse>(AppDbContext db)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken ct)
    {
        if (request is IQuery) return await next(ct);

        var response = await next(ct);
        if (response is IErrorOr { IsError: true }) return response;

        await db.SaveChangesAsync(ct);
        return response;
    }
}