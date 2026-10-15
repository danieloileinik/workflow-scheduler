using System.Text.Json;
using Confluent.Kafka;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxDispatcher(AppDbContext db, IMessagePublisher publisher, ILogger<OutboxDispatcher> logger)
{
    public const int BatchSize = 100;
    private const int MaxAttempts = 8;
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(10);

    public async Task<int> DispatchAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var claimId = Guid.NewGuid();
        var claimedUntil = now + Lease;
        var pending = await ClaimAsync(claimId, now, claimedUntil, ct);
        if (pending.Count == 0) return 0;

        try
        {
            var commands = new Dictionary<Guid, StepCommandEvent>();
            foreach (var message in pending)
                try
                {
                    commands.Add(message.Id, JsonSerializer.Deserialize<StepCommandEvent>(message.Payload)
                        ?? throw new JsonException("Outbox command is null"));
                }
                catch (JsonException ex)
                {
                    await RecordFailureAsync(message, claimId, ex, ct);
                }

            var processIds = commands.Values.Select(command => command.ProcessInstanceId).Distinct().ToArray();
            var statuses = await db.ProcessInstances.Where(instance => processIds.AsEnumerable().Contains(instance.Id))
                .Select(instance => new { instance.Id, instance.Status })
                .ToDictionaryAsync(instance => instance.Id, instance => instance.Status, ct);

            foreach (var message in pending)
            {
                ct.ThrowIfCancellationRequested();
                // Leave enough lease time for one bounded publish. Unfinished claims are released below.
                if (DateTime.UtcNow + PublishTimeout >= claimedUntil) break;
                if (!commands.TryGetValue(message.Id, out var command)) continue;
                try
                {
                    var suppressed = !statuses.TryGetValue(command.ProcessInstanceId, out var status)
                        || !command.IsCompensation && status is ProcessStatus.Cancelled or ProcessStatus.Failed
                            or ProcessStatus.Completed;
                    if (!suppressed)
                    {
                        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        timeout.CancelAfter(PublishTimeout);
                        await publisher.PublishAsync(message.Type, command.ProcessInstanceId.ToString(), message.Payload,
                            timeout.Token);
                    }

                    await db.OutboxMessages.Where(candidate => candidate.Id == message.Id && candidate.ClaimId == claimId)
                        .ExecuteUpdateAsync(update => update
                            .SetProperty(candidate => candidate.ProcessedAt, DateTime.UtcNow)
                            .SetProperty(candidate => candidate.Error,
                                suppressed ? "Suppressed because the process is terminal or missing" : null)
                            .SetProperty(candidate => candidate.Attempts, message.Attempts + (suppressed ? 0 : 1))
                            .SetProperty(candidate => candidate.NextAttemptAt, (DateTime?)null)
                            .SetProperty(candidate => candidate.ClaimId, (Guid?)null)
                            .SetProperty(candidate => candidate.ClaimedUntil, (DateTime?)null), ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    await RecordFailureAsync(message, claimId, ex, ct);
                }
            }

            return pending.Count;
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await db.OutboxMessages.Where(message => message.ClaimId == claimId)
                    .ExecuteUpdateAsync(update => update.SetProperty(message => message.ClaimId, (Guid?)null)
                        .SetProperty(message => message.ClaimedUntil, (DateTime?)null), cleanup.Token);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Outbox claim {ClaimId} could not be released; it expires at {ClaimedUntil}",
                    claimId, claimedUntil);
            }
        }
    }

    private async Task<List<OutboxMessage>> ClaimAsync(Guid claimId, DateTime now, DateTime claimedUntil,
        CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var pending = await db.OutboxMessages.FromSqlInterpolated($"""
                SELECT * FROM outbox_messages
                WHERE "ProcessedAt" IS NULL AND "FailedAt" IS NULL
                  AND ("NextAttemptAt" IS NULL OR "NextAttemptAt" <= {now})
                  AND ("ClaimedUntil" IS NULL OR "ClaimedUntil" <= {now})
                ORDER BY COALESCE("NextAttemptAt", "CreatedAt"), "CreatedAt", "Id"
                LIMIT {BatchSize} FOR UPDATE SKIP LOCKED
                """)
            .AsNoTracking().ToListAsync(ct);
        if (pending.Count > 0)
        {
            var ids = pending.Select(message => message.Id).ToArray();
            await db.OutboxMessages.Where(message => ids.AsEnumerable().Contains(message.Id))
                .ExecuteUpdateAsync(update => update.SetProperty(message => message.ClaimId, claimId)
                    .SetProperty(message => message.ClaimedUntil, claimedUntil), ct);
        }

        await transaction.CommitAsync(ct);
        return pending;
    }

    private async Task RecordFailureAsync(OutboxMessage message, Guid claimId, Exception exception, CancellationToken ct)
    {
        var attempts = message.Attempts + 1;
        var failed = attempts >= MaxAttempts || exception is JsonException or ArgumentException
            || exception is KafkaException { Error.Code: ErrorCode.MsgSizeTooLarge };
        var now = DateTime.UtcNow;
        var nextAttempt = failed ? (DateTime?)null : now.AddSeconds(Math.Min(60, 1 << attempts));
        var error = exception.Message[..Math.Min(exception.Message.Length, 2048)];
        await db.OutboxMessages.Where(candidate => candidate.Id == message.Id && candidate.ClaimId == claimId)
            .ExecuteUpdateAsync(update => update.SetProperty(candidate => candidate.Attempts, attempts)
                .SetProperty(candidate => candidate.Error, error)
                .SetProperty(candidate => candidate.NextAttemptAt, nextAttempt)
                .SetProperty(candidate => candidate.FailedAt, failed ? now : (DateTime?)null)
                .SetProperty(candidate => candidate.ClaimId, (Guid?)null)
                .SetProperty(candidate => candidate.ClaimedUntil, (DateTime?)null), ct);
        if (failed) logger.LogError(exception, "Outbox message {MessageId} quarantined after {Attempts} attempts",
            message.Id, attempts);
        else logger.LogWarning(exception, "Outbox message {MessageId} will retry at {NextAttemptAt}", message.Id, nextAttempt);
    }
}
