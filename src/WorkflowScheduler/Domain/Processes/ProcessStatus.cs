namespace WorkflowScheduler.Domain.Processes;

public enum ProcessStatus
{
    Created,
    Running,
    Waiting,
    Failed,
    Completed,
    Cancelled
}