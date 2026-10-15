using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Steps.Commands.CompleteStep;

public class CompleteStepHandler(
    AppDbContext db,
    IProcessDefinitionCache cache,
    StepExecutor executor,
    SagaCoordinator saga)
    : IRequestHandler<CompleteStepCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(CompleteStepCommand request, CancellationToken ct)
    {
        if (await db.EventLogs.AnyAsync(
                e => e.ProcessInstanceId == request.ProcessInstanceId
                     && e.EventName == "CompleteStep" && e.CorrelationId == request.CorrelationId,
                ct))
            return ProcessErrors.DuplicateEvent;
        var instance = await db
            .ProcessInstances
            .Include(x => x.Steps)
            .FirstOrDefaultAsync(x => x.Id == request.ProcessInstanceId, ct);
        if (instance is null) return ProcessErrors.NotFound;
        var step = instance.Steps.FirstOrDefault(x => x.Id == request.StepInstanceId);
        if (step is null || step.StepDefinitionId != request.StepDefinitionId) return ProcessErrors.StepNotFound;
        if (step.Status != StepStatus.Running || step.RetryCount != request.Attempt)
            return Error.Conflict("ProcessStep.StaleCallback", "Step is not running this attempt");
        if (!step.IsCompensation && (instance.Status != ProcessStatus.Waiting || instance.CurrentStepId != step.Id))
            return ProcessErrors.NotWaiting;
        var definition = await cache.GetAsync(instance.DefinitionId, ct)
                         ?? await db
                             .ProcessDefinitions
                             .Include(x => x.Steps)
                             .FirstOrDefaultAsync(x => x.Id == instance.DefinitionId, ct);
        if (definition is null) return ProcessErrors.DefinitionNotFound;
        var stepDefinition = definition.FindStep(step.StepDefinitionId);
        if (stepDefinition is null) return ProcessErrors.StepNotFound;
        if (stepDefinition.Type != StepType.Call) return ProcessErrors.StepTypeMismatch;
        ErrorOr<Success> result = Result.Success;
        if (request.Success)
        {
            step.Complete(request.OutputData);
            if (step.IsCompensation)
            {
                result = await saga.CompleteAsync(instance, definition, step, ct);
            }
            else
            {
                var resume = instance.Resume();
                if (resume.IsError) return resume;
                result = stepDefinition.NextStepId is null
                    ? instance.Complete()
                    : await executor.ExecuteAsync(definition, instance, stepDefinition.NextStepId.Value, ct);
            }
        }
        else if (!step.TryScheduleRetry(TimeSpan.FromSeconds(30)))
        {
            var error = request.OutputData ?? "Step failed after max retries";
            step.Fail(error[..Math.Min(error.Length, 2048)]);
            if (step.IsCompensation)
            {
                var log = await db.SagaCompensationLogs.FirstOrDefaultAsync(x => x.StepInstanceId == step.Id, ct);
                if (log is null) return ProcessErrors.StepNotFound;
                log.Fail(step.Error!);
            }
            else
            {
                var fail = instance.Fail();
                if (fail.IsError) return fail;
                result = await saga.ScheduleAsync(instance, definition, ct);
            }
        }

        if (result.IsError) return result;
        db.EventLogs.Add(new EventLog(instance.Id, "CompleteStep", request.CorrelationId));
        return Result.Success;
    }
}