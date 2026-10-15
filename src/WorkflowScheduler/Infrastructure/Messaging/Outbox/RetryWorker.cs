using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class RetryWorker(IServiceScopeFactory scopes, ILogger<RetryWorker> logger) : BackgroundService
{
    public async Task<int> DispatchAsync(CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var now = DateTime.UtcNow;
        var ids = await db.StepInstances
            .Where(step => step.Status == StepStatus.Pending && step.NextRetryAt <= now
                && db.ProcessInstances.Any(process => process.Id == step.ProcessInstanceId
                    && (step.IsCompensation
                        ? process.Status == ProcessStatus.Cancelled || process.Status == ProcessStatus.Failed
                        : process.Status == ProcessStatus.Waiting && process.CurrentStepId == step.Id)))
            .GroupBy(step => step.ProcessInstanceId)
            .Select(group => new { Id = group.Key, Due = group.Min(step => step.NextRetryAt) })
            .OrderBy(process => process.Due).ThenBy(process => process.Id)
            .Take(100).Select(process => process.Id).ToArrayAsync(ct);
        foreach (var id in ids)
        {
            var failed = false;
            try
            {
                await using var commandScope = scopes.CreateAsyncScope();
                var mediator = commandScope.ServiceProvider.GetRequiredService<IMediator>();
                var result = await mediator.Send(new RetryStepsCommand(id), ct);
                if (result.IsError)
                {
                    failed = true;
                    logger.LogWarning("Retry dispatch rejected for {ProcessInstanceId}: {Errors}", id,
                        string.Join("; ", result.Errors.Select(error => $"{error.Code}: {error.Description}")));
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                failed = true;
                logger.LogError(ex, "Retry dispatch failed for {ProcessInstanceId}", id);
            }

            if (failed)
                await db.StepInstances.Where(step => step.ProcessInstanceId == id && step.Status == StepStatus.Pending
                        && step.NextRetryAt <= now)
                    .ExecuteUpdateAsync(update => update.SetProperty(step => step.NextRetryAt,
                        DateTime.UtcNow.AddSeconds(30)), ct);
        }

        return ids.Length;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (await DispatchAsync(stoppingToken) == 100) continue;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Retry dispatch cycle failed");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}