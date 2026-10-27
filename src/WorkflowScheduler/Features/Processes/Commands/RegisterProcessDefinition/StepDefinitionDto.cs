using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public record StepDefinitionDto(
    Guid Id,
    string Name,
    StepType Type,
    string Metadata,
    Guid? NextStepId,
    Guid? CompensationStepId);
