using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;
using WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Common.Behaviors;

public class TransactionBehavior<TRequest, TResponse>(AppDbContext db)
    : IPipelineBehavior<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken ct)
    {
        if (request is IQuery) return await next(ct);
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        // A transaction-scoped PostgreSQL lock survives slow requests and releases only on commit/rollback.
        var key = request switch
        {
            IProcessCommand command => $"process:{command.ProcessInstanceId}",
            IIdempotentCommand command => $"start:{command.IdempotencyKey}",
            RegisterProcessDefinitionCommand command =>
                $"definition:{command.Name}:{command.Version}",
            _ => null
        };
        if (key is not null && db.Database.IsNpgsql())
        {
            var lockId = BinaryPrimitives.ReadInt64BigEndian(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
            await db.Database.ExecuteSqlInterpolatedAsync($"SELECT pg_advisory_xact_lock({lockId})", ct);
            if (request is RegisterProcessDefinitionCommand registration)
            {
                var stepLocks = registration
                    .Steps
                    .Select(step =>
                        BinaryPrimitives.ReadInt64BigEndian(
                            SHA256.HashData(
                                Encoding.UTF8.GetBytes($"step-definition:{step.Id}"))))
                    .Distinct()
                    .Order()
                    .ToArray();
                // Acquire shared step identities in a stable order across different definitions.
                await db.Database.ExecuteSqlInterpolatedAsync(
                    $"SELECT pg_advisory_xact_lock(lock_id) FROM unnest({stepLocks}) AS locks(lock_id) ORDER BY lock_id",
                    ct);
            }
        }

        try
        {
            var response = await next(ct);
            if (response is IErrorOr { IsError: true })
            {
                await transaction.RollbackAsync(ct);
                db.ChangeTracker.Clear();
                return response;
            }

            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return response;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            db.ChangeTracker.Clear();
            throw;
        }
    }
}