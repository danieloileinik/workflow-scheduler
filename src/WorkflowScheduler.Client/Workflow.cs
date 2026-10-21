namespace WorkflowScheduler.Client;

public static class Workflow
{
    public static WorkflowBuilder Define(string name, int version = 1)
    {
        Guard.Text(name, nameof(name), 256);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        return new WorkflowBuilder(new WorkflowBuilder.DefinitionState(name, version), true);
    }
}