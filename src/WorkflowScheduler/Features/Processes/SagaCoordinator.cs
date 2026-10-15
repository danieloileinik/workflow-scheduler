using ErrorOr;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes;

public class SagaCoordinator(AppDbContext db, StepExecutor executor)
{
    public async Task<ErrorOr<Success>> ScheduleAsync(ProcessInstance instance,
        ProcessDefinition definition,
        CancellationToken ct)
    {
        var existing = await db
            .SagaCompensationLogs
            .Where(x => x.ProcessInstanceId == instance.Id)
            .ToListAsync(ct);
        var scheduled = existing.Select(log => log.OriginalStepInstanceId).ToHashSet();
        foreach (var step in instance.GetCompletedSteps().Reverse())
        {
            var compensationId = definition.FindStep(step.StepDefinitionId)?.CompensationStepId;
            if (compensationId is null || !scheduled.Add(step.Id)) continue;
            var log = new SagaCompensationLog(instance.Id, compensationId.Value, step.Id, existing.Count);
            db.SagaCompensationLogs.Add(log);
            existing.Add(log);
        }

        return await StartNextAsync(instance, definition, existing, ct);
    }

    public async Task<ErrorOr<Success>> CompleteAsync(ProcessInstance instance,
        ProcessDefinition definition,
        StepInstance step,
        CancellationToken ct)
    {
        var logs = await db
            .SagaCompensationLogs
            .Where(x => x.ProcessInstanceId == instance.Id)
            .ToListAsync(ct);
        var log = logs.FirstOrDefault(x => x.StepInstanceId == step.Id);
        if (log is null || log.Status != CompensationStatus.Running) return ProcessErrors.StepNotFound;
        var compensate = instance.Steps.First(x => x.Id == log.OriginalStepInstanceId).MarkCompensated();
        if (compensate.IsError) return compensate;
        log.Complete();
        return await StartNextAsync(instance, definition, logs, ct);
    }

    private async Task<ErrorOr<Success>> StartNextAsync(ProcessInstance instance,
        ProcessDefinition definition,
        List<SagaCompensationLog> logs,
        CancellationToken ct)
    {
        if (logs.Any(x => x.Status is CompensationStatus.Running or CompensationStatus.Failed)) return Result.Success;
        var next = logs.Where(x => x.Status == CompensationStatus.Pending).MinBy(x => x.Sequence);
        if (next is null) return Result.Success;
        var target = definition.FindStep(next.StepDefinitionId);
        if (target is null || target.Type != StepType.Call) return ProcessErrors.StepTypeMismatch;
        var step = instance.AddCompensationStep(target.Id);
        next.MarkRunning(step.Id);
        return await executor.RunAsync(definition, instance, target, step, ct);
    }
}
