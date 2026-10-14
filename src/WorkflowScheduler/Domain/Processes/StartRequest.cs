namespace WorkflowScheduler.Domain.Processes;

public class StartRequest
{
    public Guid Id { get; init; }
    public Guid ProcessInstanceId { get; init; }
    public required string RequestHash { get; init; }
}