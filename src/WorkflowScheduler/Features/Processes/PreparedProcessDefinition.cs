using System.Collections.Frozen;
using System.Collections.Immutable;
using ErrorOr;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Features.Steps.Strategies;

namespace WorkflowScheduler.Features.Processes;

public sealed class PreparedProcessDefinition
{
    private readonly FrozenDictionary<Guid, PreparedStepDefinition> _stepsById;

    public PreparedProcessDefinition(ProcessDefinition definition)
    {
        Id = definition.Id;
        Steps = [.. definition.Steps.OrderBy(step => step.Position).Select(step => new PreparedStepDefinition(step))];
        _stepsById = Steps.ToFrozenDictionary(step => step.Id);
    }

    public Guid Id { get; }
    public ImmutableArray<PreparedStepDefinition> Steps { get; }

    public ErrorOr<PreparedStepDefinition> GetFirstStep()
    {
        return Steps.IsEmpty ? ProcessErrors.NoStepsDefined : Steps[0];
    }

    public PreparedStepDefinition? FindStep(Guid stepId)
    {
        return _stepsById.GetValueOrDefault(stepId);
    }
}