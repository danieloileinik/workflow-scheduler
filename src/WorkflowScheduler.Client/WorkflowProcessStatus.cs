namespace WorkflowScheduler.Client;

public enum WorkflowProcessStatus
{
    Created,
    Running,
    Waiting,
    Failed,
    Completed,
    Cancelled
}