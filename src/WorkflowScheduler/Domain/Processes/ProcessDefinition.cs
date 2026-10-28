using System.Collections.Frozen;
using System.Collections.ObjectModel;
using ErrorOr;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Domain.Processes;

public sealed class ProcessDefinition
{
    private readonly List<StepDefinition> _steps = [];
    private readonly ReadOnlyCollection<StepDefinition> _readOnlySteps;
    private readonly Lazy<FrozenDictionary<Guid, StepDefinition>> _stepsById;
    private readonly Lazy<StepDefinition?> _firstStep;

    private ProcessDefinition(Guid id, string name, int version, DateTime createdAt)
    {
        Id = id;
        Name = name;
        Version = version;
        CreatedAt = createdAt;
        _readOnlySteps = _steps.AsReadOnly();
        _stepsById = new Lazy<FrozenDictionary<Guid, StepDefinition>>(
            () => _steps.ToFrozenDictionary(step => step.Id));
        _firstStep = new Lazy<StepDefinition?>(() => _steps.MinBy(step => step.Position));
    }

    public ProcessDefinition(
        Guid id,
        string name,
        int version,
        IEnumerable<StepDefinition> steps,
        DateTime? createdAt = null)
        : this(id, name, version, createdAt ?? DateTime.UtcNow)
    {
        ArgumentNullException.ThrowIfNull(steps);
        _steps.AddRange(steps.OrderBy(step => step.Position));
    }

    public Guid Id { get; }
    public string Name { get; }
    public int Version { get; }
    public DateTime CreatedAt { get; }
    public IReadOnlyList<StepDefinition> Steps => _readOnlySteps;

    public ErrorOr<StepDefinition> GetFirstStep()
    {
        return _firstStep.Value is { } first ? first : ProcessErrors.NoStepsDefined;
    }

    public StepDefinition? FindStep(Guid stepId)
    {
        return _stepsById.Value.GetValueOrDefault(stepId);
    }

    internal void InitializeExecution()
    {
        // EF populates the private navigation before the detached definition enters the shared cache.
        _ = _stepsById.Value;
        _ = _firstStep.Value;

        foreach (StepDefinition step in _steps)
            _ = step.ParsedMetadata;
    }
}
