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
    CachedProcessDefinitionProvider definitions,
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
        var definition = await definitions.GetOrLoadAsync(instance.DefinitionId, ct);
        if (definition is null) return ProcessErrors.DefinitionNotFound;
        var stepDefinition = definition.FindStep(step.StepDefinitionId);
        if (stepDefinition is null) return ProcessErrors.StepNotFound;
        if (stepDefinition.Type != StepType.Call) return ProcessErrors.StepTypeMismatch;
        ErrorOr<Success> result = Result.Success;
        if (request.Success)
        {
            var complete = step.Complete(request.OutputData);
            if (complete.IsError) return complete;
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
        else
        {
            result = await HandleFailureAsync(instance, definition, step, request.OutputData, ct);
        }

        if (result.IsError) return result;
        db.EventLogs.Add(new EventLog(instance.Id, "CompleteStep", request.CorrelationId));
        return Result.Success;
    }

    private async Task<ErrorOr<Success>> HandleFailureAsync(ProcessInstance instance,
        ProcessDefinition definition,
        StepInstance step,
        string? outputData,
        CancellationToken ct)
    {
        var retry = step.TryScheduleRetry(TimeSpan.FromSeconds(30));
        if (retry.IsError) return retry.Errors;
        if (retry.Value) return Result.Success;

        var error = outputData ?? "Step failed after max retries";
        var failStep = step.Fail(error[..Math.Min(error.Length, 2048)]);
        if (failStep.IsError) return failStep;
        if (step.IsCompensation)
        {
            var log = await db.SagaCompensationLogs.FirstOrDefaultAsync(x => x.StepInstanceId == step.Id, ct);
            if (log is null) return ProcessErrors.StepNotFound;
            log.Fail(step.Error!);
            return Result.Success;
        }

        var fail = instance.Fail();
        if (fail.IsError) return fail;
        return await saga.ScheduleAsync(instance, definition, ct);
    }
}