using ErrorOr;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Domain.Processes;

public class ProcessDefinition(Guid id, string name, int version)
{
    private readonly List<ProcessStepDefinition> _steps = [];

    public ProcessDefinition(string name, int version) : this(Guid.NewGuid(), name, version)
    {
    }

    public Guid Id { get; } = id;
    public string Name { get; } = name;
    public int Version { get; } = version;
    public DateTime CreatedAt { get; } = DateTime.UtcNow;
    public IEnumerable<ProcessStepDefinition> Steps => _steps;

    public void AddStep(ProcessStepDefinition step)
    {
        _steps.Add(step);
    }

    public ErrorOr<ProcessStepDefinition> GetFirstStep()
    {
        if (_steps.Count == 0) return ProcessErrors.NoStepsDefined;
        return _steps[0];
    }

    public ProcessStepDefinition? FindStep(Guid stepId)
    {
        return _steps.FirstOrDefault(s => s.Id == stepId);
    }
}