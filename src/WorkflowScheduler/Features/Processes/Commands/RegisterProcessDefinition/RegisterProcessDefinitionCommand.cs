using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public sealed record RegisterProcessDefinitionCommand : IRequest<ErrorOr<RegisterProcessDefinitionResponse>>
{
    public RegisterProcessDefinitionCommand(string name, int version, IReadOnlyList<StepDefinitionDto> steps)
    {
        Definition = new ProcessDefinition(name, version);

        foreach (StepDefinitionDto step in steps)
            Definition.AddStep(new StepDefinition(
                step.Id,
                Definition.Id,
                step.Name,
                step.Type,
                step.Metadata,
                step.NextStepId,
                step.CompensationStepId));
    }

    public ProcessDefinition Definition { get; }
    public string Name => Definition.Name;
    public int Version => Definition.Version;
    public IEnumerable<StepDefinition> Steps => Definition.Steps;
}
