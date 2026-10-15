using ErrorOr;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Domain.Processes;

public class ProcessDefinition(Guid id, string name, int version)
{
    private readonly List<StepDefinition> _steps = [];
    private Dictionary<Guid, StepDefinition>? _stepsById;
    private int _indexedStepCount;

    public ProcessDefinition(string name, int version) : this(Guid.NewGuid(), name, version)
    {
    }

    public Guid Id { get; } = id;
    public string Name { get; } = name;
    public int Version { get; } = version;
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public IEnumerable<StepDefinition> Steps => _steps;

    public void AddStep(StepDefinition step)
    {
        step.SetPosition(_steps.Count);
        _steps.Add(step);
    }

    public ErrorOr<StepDefinition> GetFirstStep()
    {
        if (_steps.Count == 0) return ProcessErrors.NoStepsDefined;
        return _steps.MinBy(s => s.Position)!;
    }

    public StepDefinition? FindStep(Guid stepId)
    {
        if (_stepsById is null || _indexedStepCount != _steps.Count)
        {
            _stepsById = new Dictionary<Guid, StepDefinition>(_steps.Count);
            foreach (var step in _steps) _stepsById.TryAdd(step.Id, step);
            _indexedStepCount = _steps.Count;
        }

        return _stepsById.GetValueOrDefault(stepId);
    }
}