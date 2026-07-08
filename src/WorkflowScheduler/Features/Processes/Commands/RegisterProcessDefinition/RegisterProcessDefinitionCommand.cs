using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public record StepDefinitionDto(
    Guid Id,
    string Name,
    StepType Type,
    string Metadata,
    Guid? NextStepId,
    Guid? CompensationStepId);

public record RegisterProcessDefinitionCommand(
    string Name,
    int Version,
    IReadOnlyList<StepDefinitionDto> Steps) : IRequest<ErrorOr<RegisterProcessDefinitionResponse>>;

public record RegisterProcessDefinitionResponse(Guid DefinitionId);