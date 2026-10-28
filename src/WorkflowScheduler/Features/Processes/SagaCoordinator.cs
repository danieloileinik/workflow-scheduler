using ErrorOr;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes;

public class SagaCoordinator(AppDbContext db, StepExecutor executor)
{
    public async Task<ErrorOr<Success>> ScheduleAsync(ProcessInstance instance,
        PreparedProcessDefinition definition,
        CancellationToken ct)
    {
        var existing = await db
            .SagaCompensationLogs
            .Where(x => x.ProcessInstanceId == instance.Id)
            .ToListAsync(ct);

        var scheduled = existing.Select(log => log.OriginalStepInstanceId).ToHashSet();
        var compensations = definition
            .Steps
            .Where(step => step.CompensationStepId.HasValue)
            .ToDictionary(step => step.Id, step => step.CompensationStepId!.Value);

        var compensatableIds = compensations.Keys.ToArray();

        var completed = await db
            .StepInstances
            .Where(step => step.ProcessInstanceId == instance.Id && !step.IsCompensation
                                                                 && step.Status == StepStatus.Completed &&
                                                                 compensatableIds
                                                                     .AsEnumerable()
                                                                     .Contains(step.StepDefinitionId))
            .OrderByDescending(step => step.Sequence)
            .Select(step => new { step.Id, step.StepDefinitionId })
            .ToListAsync(ct);

        foreach (var step in completed)
        {
            if (!scheduled.Add(step.Id))
                continue;

            var log = new SagaCompensationLog(
                instance.Id,
                compensations[step.StepDefinitionId],
                step.Id,
                existing.Count);
            db.SagaCompensationLogs.Add(log);
            existing.Add(log);
        }

        if (existing.Any(log => log.Status is CompensationStatus.Running or CompensationStatus.Failed))
            return Result.Success;

        return StartNext(
            instance,
            definition,
            existing.Where(log => log.Status == CompensationStatus.Pending).MinBy(log => log.Sequence),
            ct);
    }

    public async Task<ErrorOr<Success>> CompleteAsync(ProcessInstance instance,
        PreparedProcessDefinition definition,
        StepInstance step,
        CancellationToken ct)
    {
        var log = await db
            .SagaCompensationLogs
            .FirstOrDefaultAsync(
                candidate => candidate.ProcessInstanceId == instance.Id
                             && candidate.StepInstanceId == step.Id && candidate.Status == CompensationStatus.Running,
                ct);
        if (log is null)
            return ProcessErrors.StepNotFound;

        var compensated = await db
            .StepInstances
            .Where(original => original.ProcessInstanceId == instance.Id
                               && original.Id == log.OriginalStepInstanceId && original.Status == StepStatus.Completed)
            .ExecuteUpdateAsync(update => update.SetProperty(original => original.Status, StepStatus.Compensated), ct);
        if (compensated != 1)
            return ProcessErrors.StepNotFound;

        log.Complete();

        if (await db.SagaCompensationLogs.AnyAsync(
                candidate => candidate.ProcessInstanceId == instance.Id
                             && candidate.Id != log.Id
                             && (candidate.Status == CompensationStatus.Running ||
                                 candidate.Status == CompensationStatus.Failed),
                ct))
            return Result.Success;

        var next = await db
            .SagaCompensationLogs
            .Where(candidate =>
                candidate.ProcessInstanceId == instance.Id && candidate.Status == CompensationStatus.Pending)
            .OrderBy(candidate => candidate.Sequence)
            .FirstOrDefaultAsync(ct);

        return StartNext(instance, definition, next, ct);
    }

    private ErrorOr<Success> StartNext(ProcessInstance instance,
        PreparedProcessDefinition definition,
        SagaCompensationLog? next,
        CancellationToken ct)
    {
        if (next is null)
            return Result.Success;

        var target = definition.FindStep(next.StepDefinitionId);

        if (target is null || target.Type != StepType.Call)
            return ProcessErrors.StepTypeMismatch;

        var step = instance.AddCompensationStep(target.Id);

        next.MarkRunning(step.Id);

        return executor.Run(definition, instance, target, step, ct);
    }
}