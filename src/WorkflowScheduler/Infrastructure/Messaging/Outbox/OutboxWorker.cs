using Microsoft.EntityFrameworkCore;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxWorker(IServiceScopeFactory scopeFactory, KafkaPublisher publisher, ILogger<OutboxWorker> logger)
    : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox dispatch cycle failed");
            }

            await Task.Delay(PollInterval, stoppingToken);
        }
    }

    private async Task ProcessBatchAsync(CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var pending = await db
            .OutboxMessages
            .Where(m => m.ProcessedAt == null)
            .OrderBy(m => m.CreatedAt)
            .Take(100)
            .ToListAsync(ct);

        if (pending.Count == 0) return;

        foreach (var message in pending)
            try
            {
                await publisher.PublishAsync(message.Type, message.Id.ToString(), message.Payload, ct);
                message.ProcessedAt = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                message.Error = ex.Message;
            }

        await db.SaveChangesAsync(ct);
    }
}