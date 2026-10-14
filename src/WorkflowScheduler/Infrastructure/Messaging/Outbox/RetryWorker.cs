using MediatR;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes.Commands.RetrySteps;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class RetryWorker(IServiceScopeFactory scopes, ILogger<RetryWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var ids = await db
                    .StepInstances
                    .Where(x => x.Status == StepStatus.Pending
                                && x.NextRetryAt <= DateTime.UtcNow)
                    .Select(x => x.ProcessInstanceId)
                    .Distinct()
                    .Take(100)
                    .ToArrayAsync(stoppingToken);
                foreach (var id in ids)
                {
                    await using var commandScope = scopes.CreateAsyncScope();
                    var mediator = commandScope.ServiceProvider.GetRequiredService<IMediator>();
                    await mediator.Send(new RetryStepsCommand(id), stoppingToken);
                }
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