namespace WorkflowScheduler.Client;

public sealed record RegisteredWorkflow(Guid Id, string Name, int Version);