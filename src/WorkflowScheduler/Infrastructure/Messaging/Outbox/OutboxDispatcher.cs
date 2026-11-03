using System.Text.Json;
using Microsoft.Extensions.Options;
using WorkflowScheduler.Common;
using WorkflowScheduler.Contracts.Grpc;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxDispatcher(
    AppDbContext db,
    IMessagePublisher publisher,
    ILogger<OutboxDispatcher> logger,
    IOptions<CallRecoveryOptions> recovery
)
{
    public const int BatchSize = 100;
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(5);

    public async Task<int> DispatchAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var claimId = Guid.NewGuid();
        var claimedUntil = now + Lease;
        var pending = await ClaimAsync(claimId, now, claimedUntil, ct);
        if (pending.Count == 0)
            return 0;

        try
        {
            var commands = await DeserializeBatchAsync(pending, claimId, ct);
            await DispatchBatchAsync(commands, claimId, claimedUntil, ct);

            return pending.Count;
        }
        finally
        {
            await ReleaseClaimsAsync(claimId, claimedUntil);
        }
    }

    private async Task<
        List<(OutboxMessage Message, ExecuteStepCommand Command)>
    > DeserializeBatchAsync(
        IReadOnlyList<OutboxMessage> pending,
        Guid claimId,
        CancellationToken ct
    )
    {
        List<(OutboxMessage Message, ExecuteStepCommand Command)> commands = [];

        foreach (var message in pending)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var command =
                    JsonSerializer.Deserialize<ExecuteStepCommand>(message.Payload)
                    ?? throw new JsonException("Outbox command is null");
                commands.Add((message, command));
            }
            catch (JsonException ex)
            {
                await RecordFailureAsync(message, claimId, ex, ct, permanent: true);
            }
        }

        return commands;
    }

    private async Task DispatchBatchAsync(
        IEnumerable<(OutboxMessage Message, ExecuteStepCommand Command)> commands,
        Guid claimId,
        DateTime claimedUntil,
        CancellationToken ct
    )
    {
        foreach (var (message, command) in commands)
        {
            ct.ThrowIfCancellationRequested();

            // Leave enough lease time for one bounded publish. Unfinished claims are released by the caller.
            if (DateTime.UtcNow + PublishTimeout >= claimedUntil)
                break;

            try
            {
                await DispatchMessageAsync(message, command, claimId, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                await RecordFailureAsync(
                    message,
                    claimId,
                    ex,
                    ct,
                    permanent: ex is MessageRejectedException
                );
            }
        }
    }

    private async Task DispatchMessageAsync(
        OutboxMessage message,
        ExecuteStepCommand command,
        Guid claimId,
        CancellationToken ct
    )
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(PublishTimeout);

        await using var transaction = await db.Database.BeginTransactionAsync(timeout.Token);

        // Serialize one bounded delivery with process mutations, including cancellation.
        // A later message in the batch observes any cancellation committed between deliveries.
        var lockId = AdvisoryLock.ForProcess(command.ProcessInstanceId);
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"SELECT pg_advisory_xact_lock({lockId})",
            timeout.Token
        );

        var now = DateTime.UtcNow;
        var ownsClaim = await db.OutboxMessages.AnyAsync(
            candidate =>
                candidate.Id == message.Id
                && candidate.ClaimId == claimId
                && candidate.ClaimedUntil > now + PublishTimeout,
            timeout.Token
        );
        if (!ownsClaim)
            return;

        var status = await db
            .ProcessInstances.Where(instance => instance.Id == command.ProcessInstanceId)
            .Select(instance => (ProcessStatus?)instance.Status)
            .SingleOrDefaultAsync(timeout.Token);
        var suppressed =
            status is null
            || (
                !command.IsCompensation
                && status
                    is ProcessStatus.Cancelled
                        or ProcessStatus.Failed
                        or ProcessStatus.Completed
            );

        if (!suppressed)
            await publisher.PublishAsync(
                message.Type,
                command.ProcessInstanceId.ToString(),
                message.Payload,
                timeout.Token
            );

        var acknowledged = await AcknowledgeAsync(message, claimId, suppressed, timeout.Token);
        if (!suppressed && acknowledged)
            await ScheduleRecoveryAsync(command, timeout.Token);

        await transaction.CommitAsync(timeout.Token);
    }

    private async Task<bool> AcknowledgeAsync(
        OutboxMessage message,
        Guid claimId,
        bool suppressed,
        CancellationToken ct
    )
    {
        var updated = await db
            .OutboxMessages.Where(candidate =>
                candidate.Id == message.Id && candidate.ClaimId == claimId
            )
            .ExecuteUpdateAsync(
                update =>
                    update
                        .SetProperty(candidate => candidate.ProcessedAt, DateTime.UtcNow)
                        .SetProperty(
                            candidate => candidate.Error,
                            suppressed
                                ? "Suppressed because the process is terminal or missing"
                                : null
                        )
                        .SetProperty(
                            candidate => candidate.Attempts,
                            (int)
                                Math.Min(
                                    (long)message.Attempts + (suppressed ? 0 : 1),
                                    int.MaxValue
                                )
                        )
                        .SetProperty(candidate => candidate.NextAttemptAt, (DateTime?)null)
                        .SetProperty(candidate => candidate.ClaimId, (Guid?)null)
                        .SetProperty(candidate => candidate.ClaimedUntil, (DateTime?)null),
                ct
            );

        return updated == 1;
    }

    private async Task ScheduleRecoveryAsync(ExecuteStepCommand command, CancellationToken ct)
    {
        var nextRecoveryAt = DateTime.UtcNow.Add(recovery.Value.CallbackTimeout);

        await db
            .StepInstances.Where(step =>
                step.Id == command.StepInstanceId
                && step.ProcessInstanceId == command.ProcessInstanceId
                && step.StepDefinitionId == command.StepDefinitionId
                && step.Status == StepStatus.Running
                && step.RetryCount == command.Attempt
            )
            .ExecuteUpdateAsync(
                update => update.SetProperty(step => step.NextRecoveryAt, nextRecoveryAt),
                ct
            );
    }

    private async Task<List<OutboxMessage>> ClaimAsync(
        Guid claimId,
        DateTime now,
        DateTime claimedUntil,
        CancellationToken ct
    )
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var pending = await db
            .OutboxMessages.FromSqlInterpolated(
                $"""
                SELECT * FROM outbox_messages
                WHERE "ProcessedAt" IS NULL AND "FailedAt" IS NULL
                  AND ("NextAttemptAt" IS NULL OR "NextAttemptAt" <= {now})
                  AND ("ClaimedUntil" IS NULL OR "ClaimedUntil" <= {now})
                ORDER BY COALESCE("NextAttemptAt", "CreatedAt"), "CreatedAt", "Id"
                LIMIT {BatchSize} FOR UPDATE SKIP LOCKED
                """
            )
            .AsNoTracking()
            .ToListAsync(ct);
        if (pending.Count > 0)
        {
            var ids = pending.Select(message => message.Id).ToArray();
            await db
                .OutboxMessages.Where(message => ids.AsEnumerable().Contains(message.Id))
                .ExecuteUpdateAsync(
                    update =>
                        update
                            .SetProperty(message => message.ClaimId, claimId)
                            .SetProperty(message => message.ClaimedUntil, claimedUntil),
                    ct
                );
        }

        await transaction.CommitAsync(ct);

        return pending;
    }

    private async Task RecordFailureAsync(
        OutboxMessage message,
        Guid claimId,
        Exception exception,
        CancellationToken ct,
        bool permanent
    )
    {
        var attempts = (int)Math.Min((long)message.Attempts + 1, int.MaxValue);
        var now = DateTime.UtcNow;
        var nextAttempt = permanent
            ? (DateTime?)null
            : now.AddSeconds(Math.Min(60, 1 << Math.Min(attempts, 6)));
        var error = exception.Message[..Math.Min(exception.Message.Length, 2048)];

        var updated = await db
            .OutboxMessages.Where(candidate =>
                candidate.Id == message.Id && candidate.ClaimId == claimId
            )
            .ExecuteUpdateAsync(
                update =>
                    update
                        .SetProperty(candidate => candidate.Attempts, attempts)
                        .SetProperty(candidate => candidate.Error, error)
                        .SetProperty(candidate => candidate.NextAttemptAt, nextAttempt)
                        .SetProperty(candidate => candidate.FailedAt, permanent ? now : null)
                        .SetProperty(candidate => candidate.ClaimId, (Guid?)null)
                        .SetProperty(candidate => candidate.ClaimedUntil, (DateTime?)null),
                ct
            );

        if (updated == 0)
            return;

        if (permanent)
            logger.LogError(
                exception,
                "Outbox message {MessageId} quarantined after {Attempts} attempts",
                message.Id,
                attempts
            );
        else
            logger.LogWarning(
                exception,
                "Outbox message {MessageId} will retry at {NextAttemptAt}",
                message.Id,
                nextAttempt
            );
    }

    private async Task ReleaseClaimsAsync(Guid claimId, DateTime claimedUntil)
    {
        using var cleanup = new CancellationTokenSource(CleanupTimeout);

        try
        {
            await db
                .OutboxMessages.Where(message => message.ClaimId == claimId)
                .ExecuteUpdateAsync(
                    update =>
                        update
                            .SetProperty(message => message.ClaimId, (Guid?)null)
                            .SetProperty(message => message.ClaimedUntil, (DateTime?)null),
                    cleanup.Token
                );
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Outbox claim {ClaimId} could not be released; it expires at {ClaimedUntil}",
                claimId,
                claimedUntil
            );
        }
    }
}
