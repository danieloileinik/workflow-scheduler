using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public sealed record RegisterProcessDefinitionCommand : IRequest<ErrorOr<RegisterProcessDefinitionResponse>>
{
    public RegisterProcessDefinitionCommand(string name, int version, IReadOnlyList<StepDefinitionDto> steps)
    {
        var id = Guid.NewGuid();
        Definition = new ProcessDefinition(
            id,
            name,
            version,
            steps.Select((step, position) => new StepDefinition(
                step.Id,
                id,
                step.Name,
                step.Type,
                step.Metadata,
                step.NextStepId,
                step.CompensationStepId,
                position)));
    }

    public ProcessDefinition Definition { get; }
    public string Name => Definition.Name;
    public int Version => Definition.Version;
    public IEnumerable<StepDefinition> Steps => Definition.Steps;
}