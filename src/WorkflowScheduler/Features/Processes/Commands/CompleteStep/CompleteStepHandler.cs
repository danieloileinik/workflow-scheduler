using ErrorOr;
using MediatR;
using Microsoft.EntityFrameworkCore;
using WorkflowScheduler.Common.Abstractions;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes.StepStrategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.CompleteStep;

public class CompleteStepHandler(
    AppDbContext db,
    IProcessDefinitionCache cache,
    IProcessLock processLock,
    StepExecutor stepExecutor)
    : IRequestHandler<CompleteStepCommand, ErrorOr<Success>>
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(30);

    public async Task<ErrorOr<Success>> Handle(CompleteStepCommand request, CancellationToken ct)
    {
        await using var @lock = await processLock.TryAcquireAsync(request.ProcessInstanceId, ct);
        if (@lock is null) return ProcessErrors.LockNotAcquired;

        var duplicate = await db.EventLogs.AnyAsync(
            e => e.ProcessInstanceId == request.ProcessInstanceId
                 && e.EventName == "CompleteStep"
                 && e.CorrelationId == request.CorrelationId,
            ct);
        if (duplicate) return ProcessErrors.DuplicateEvent;

        var processInstance = await db
            .ProcessInstances
            .Include(i => i.Steps)
            .FirstOrDefaultAsync(i => i.Id == request.ProcessInstanceId, ct);
        if (processInstance is null) return ProcessErrors.NotFound;

        if (processInstance.Status is ProcessStatus.Completed or ProcessStatus.Cancelled)
            return ProcessErrors.AlreadyCompleted;

        var stepInstance = processInstance.GetCurrentStep();
        if (stepInstance is null || stepInstance.StepDefinitionId != request.StepDefinitionId)
            return ProcessErrors.StepNotFound;

        if (stepInstance.Status == StepStatus.Completed)
            return ProcessErrors.StepAlreadyCompleted;

        var processDefinition = await GetDefinitionAsync(processInstance.DefinitionId, ct);
        if (processDefinition is null) return ProcessErrors.DefinitionNotFound;

        var stepDefinition = processDefinition.FindStep(request.StepDefinitionId);
        if (stepDefinition is null) return ProcessErrors.StepNotFound;

        ErrorOr<Success> result;

        if (request.Success)
        {
            stepInstance.Complete(request.OutputData);

            var resumeResult = processInstance.Resume();
            if (resumeResult.IsError) return resumeResult.Errors;

            result = stepDefinition.NextStepId is null
                ? processInstance.Complete()
                : await stepExecutor.ExecuteAsync(
                    processDefinition,
                    processInstance,
                    stepDefinition.NextStepId.Value,
                    ct);
        }
        else
        {
            var canRetry = stepInstance.TryScheduleRetry(RetryDelay);
            if (canRetry)
            {
                var resumeResult = processInstance.Resume();
                if (resumeResult.IsError) return resumeResult.Errors;

                result = await stepExecutor.RunAsync(
                    processDefinition,
                    processInstance,
                    stepDefinition,
                    stepInstance,
                    ct);
            }
            else
            {
                stepInstance.Fail(request.OutputData ?? "Step failed after max retries");

                var failResult = processInstance.Fail("Step failed after max retries");
                if (failResult.IsError) return failResult.Errors;

                TriggerSagaRollback(processInstance, processDefinition);
                result = Result.Success;
            }
        }

        if (result.IsError) return result.Errors;

        db.EventLogs.Add(new EventLog(request.ProcessInstanceId, "CompleteStep", request.CorrelationId));

        return Result.Success;
    }

    private void TriggerSagaRollback(ProcessInstance instance, ProcessDefinition definition)
    {
        foreach (var step in instance.GetCompletedSteps().Reverse())
        {
            var stepDef = definition.FindStep(step.StepDefinitionId);
            if (stepDef?.CompensationStepId is null) continue;
            db.SagaCompensationLogs.Add(new SagaCompensationLog(instance.Id, stepDef.Id));
        }
    }

    private async Task<ProcessDefinition?> GetDefinitionAsync(Guid id, CancellationToken ct)
    {
        var cached = await cache.GetAsync(id, ct);
        if (cached is not null) return cached;

        var definition = await db
            .ProcessDefinitions
            .Include(d => d.Steps)
            .FirstOrDefaultAsync(d => d.Id == id, ct);

        if (definition is not null)
            await cache.SetAsync(definition, ct);

        return definition;
    }
}