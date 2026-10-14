using System.Text.Json;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxDispatcher(AppDbContext db, IMessagePublisher publisher)
{
    public async Task<int> DispatchAsync(CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var pending = db.Database.IsNpgsql()
            ? await db
                .OutboxMessages
                .FromSqlRaw(
                    """
                    SELECT * FROM outbox_messages WHERE "ProcessedAt" IS NULL
                    ORDER BY "CreatedAt", "Id" LIMIT 100 FOR UPDATE SKIP LOCKED
                    """)
                .ToListAsync(ct)
            : await db
                .OutboxMessages
                .Where(x => x.ProcessedAt == null)
                .OrderBy(x => x.CreatedAt)
                .ThenBy(x => x.Id)
                .Take(100)
                .ToListAsync(ct);
        foreach (var message in pending)
            try
            {
                var command = JsonSerializer.Deserialize<StepCommandEvent>(message.Payload)!;
                var status = await db
                    .ProcessInstances
                    .Where(x => x.Id == command.ProcessInstanceId)
                    .Select(x => (ProcessStatus?)x.Status)
                    .SingleOrDefaultAsync(ct);
                if (!command.IsCompensation && status is ProcessStatus.Cancelled or ProcessStatus.Failed)
                {
                    message.ProcessedAt = DateTime.UtcNow;
                    message.Error = "Suppressed because the process is terminal";
                    continue;
                }

                await publisher.PublishAsync(message.Type, command.ProcessInstanceId.ToString(), message.Payload, ct);
                message.ProcessedAt = DateTime.UtcNow;
                message.Error = null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                message.Error = ex.Message[..Math.Min(ex.Message.Length, 2048)];
            }

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        return pending.Count;
    }
}