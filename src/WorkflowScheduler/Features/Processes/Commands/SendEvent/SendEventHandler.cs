using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public class SendEventHandler(
    AppDbContext db,
    CachedProcessDefinitionProvider definitions,
    StepExecutor stepExecutor)
    : IRequestHandler<SendEventCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(SendEventCommand request, CancellationToken ct)
    {
        var duplicate = await db.EventLogs.AnyAsync(
            e => e.ProcessInstanceId == request.ProcessInstanceId
                 && e.EventName == $"event:{request.EventName.ToUpperInvariant()}"
                 && e.CorrelationId == request.CorrelationId,
            ct);
        if (duplicate)
            return ProcessErrors.DuplicateEvent;

        var processInstance = await db.LoadActiveProcessAsync(request.ProcessInstanceId, ct);
        if (processInstance is null)
            return ProcessErrors.NotFound;

        if (processInstance.Status != ProcessStatus.Waiting)
            return ProcessErrors.NotWaiting;

        var stepInstance = processInstance.GetCurrentStepInstance();
        if (stepInstance is null)
            return ProcessErrors.StepNotFound;

        var processDefinition = await definitions.GetOrLoadAsync(processInstance.DefinitionId, ct);
        if (processDefinition is null)
            return ProcessErrors.DefinitionNotFound;

        var stepDefinition = processDefinition.FindStep(stepInstance.StepDefinitionId);
        if (stepDefinition is null)
            return ProcessErrors.StepNotFound;

        if (stepDefinition.Type != StepType.WaitEvent)
            return ProcessErrors.StepTypeMismatch;

        var metadata = (StepMetadata.WaitEvent)stepDefinition.ParsedMetadata;
        if (!string.Equals(metadata.EventName, request.EventName, StringComparison.OrdinalIgnoreCase))
            return ProcessErrors.EventNameMismatch;

        var completeResult = stepInstance.Complete(request.Payload);
        if (completeResult.IsError)
            return completeResult;

        var resumeResult = processInstance.Resume();
        if (resumeResult.IsError)
            return resumeResult.Errors;

        var result = stepDefinition.NextStepId is null
            ? processInstance.Complete()
            : stepExecutor.Execute(processDefinition, processInstance, stepDefinition.NextStepId.Value, ct);

        if (result.IsError)
            return result.Errors;

        db.EventLogs.Add(
            new EventLog(
                request.ProcessInstanceId,
                $"event:{request.EventName.ToUpperInvariant()}",
                request.CorrelationId));

        return Result.Success;
    }
}