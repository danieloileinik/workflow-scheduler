namespace WorkflowScheduler.Client;

public sealed class WorkflowDefinition
{
    internal WorkflowDefinition(string name, int version, WorkflowStepDefinition[] steps)
    {
        Name = name;
        Version = version;
        Steps = Array.AsReadOnly(steps);
    }

    public string Name { get; }
    public int Version { get; }
    public IReadOnlyList<WorkflowStepDefinition> Steps { get; }
}