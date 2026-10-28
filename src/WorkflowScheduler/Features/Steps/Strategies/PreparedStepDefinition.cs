using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public sealed record PreparedStepDefinition(
    Guid Id,
    string Name,
    StepType Type,
    StepMetadata Metadata,
    Guid? NextStepId,
    Guid? CompensationStepId)
{
    public PreparedStepDefinition(StepDefinition step)
        : this(step.Id, step.Name, step.Type, step.ParsedMetadata, step.NextStepId, step.CompensationStepId)
    {
    }
}