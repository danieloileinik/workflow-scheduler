using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Steps.Commands.RetrySteps;

public class RetryStepsHandler(AppDbContext db, CachedProcessDefinitionProvider definitions, StepExecutor executor)
    : IRequestHandler<RetryStepsCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(RetryStepsCommand request, CancellationToken ct)
    {
        var instance = await db
            .ProcessInstances
            .Include(x => x.Steps)
            .FirstOrDefaultAsync(x => x.Id == request.ProcessInstanceId, ct);
        if (instance is null) return ProcessErrors.NotFound;
        var due = instance
            .Steps
            .Where(x => x.Status == StepStatus.Pending
                        && x.NextRetryAt <= DateTime.UtcNow)
            .ToList();
        if (due.Count == 0) return Result.Success;
        var definition = await definitions.GetOrLoadAsync(instance.DefinitionId, ct);
        if (definition is null) return ProcessErrors.DefinitionNotFound;
        foreach (var step in due)
        {
            if (!step.IsCompensation &&
                (instance.Status != ProcessStatus.Waiting || instance.CurrentStepId != step.Id)) continue;
            var target = definition.FindStep(step.StepDefinitionId);
            if (target is null) return ProcessErrors.StepNotFound;
            if (!step.IsCompensation)
            {
                var resume = instance.Resume();
                if (resume.IsError) return resume;
            }

            var result = await executor.RunAsync(definition, instance, target, step, ct);
            if (result.IsError) return result;
        }

        return Result.Success;
    }
}