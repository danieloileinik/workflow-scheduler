using System.Text.Json;
using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public class SendEventHandler(
    AppDbContext db,
    IProcessDefinitionCache cache,
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
        if (duplicate) return ProcessErrors.DuplicateEvent;

        var processInstance = await db
            .ProcessInstances
            .Include(i => i.Steps)
            .FirstOrDefaultAsync(i => i.Id == request.ProcessInstanceId, ct);
        if (processInstance is null) return ProcessErrors.NotFound;

        if (processInstance.Status != ProcessStatus.Waiting) return ProcessErrors.NotWaiting;

        var stepInstance = processInstance.GetCurrentStepInstance();
        if (stepInstance is null) return ProcessErrors.StepNotFound;

        var processDefinition = await GetDefinitionAsync(processInstance.DefinitionId, ct);
        if (processDefinition is null) return ProcessErrors.DefinitionNotFound;

        var stepDefinition = processDefinition.FindStep(stepInstance.StepDefinitionId);
        if (stepDefinition is null) return ProcessErrors.StepNotFound;

        if (stepDefinition.Type != StepType.WaitEvent) return ProcessErrors.StepTypeMismatch;

        var metadata = JsonSerializer.Deserialize<WaitEventMetadata>(stepDefinition.Metadata, StepMetadata.Options)!;
        if (!string.Equals(metadata.EventName, request.EventName, StringComparison.OrdinalIgnoreCase))
            return ProcessErrors.EventNameMismatch;

        var completeResult = stepInstance.Complete(request.Payload);
        if (completeResult.IsError) return completeResult;

        var resumeResult = processInstance.Resume();
        if (resumeResult.IsError) return resumeResult.Errors;

        var result = stepDefinition.NextStepId is null
            ? processInstance.Complete()
            : await stepExecutor.ExecuteAsync(processDefinition, processInstance, stepDefinition.NextStepId.Value, ct);

        if (result.IsError) return result.Errors;

        db.EventLogs.Add(
            new EventLog(
                request.ProcessInstanceId,
                $"event:{request.EventName.ToUpperInvariant()}",
                request.CorrelationId));

        return Result.Success;
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

    private record WaitEventMetadata(string EventName);
}