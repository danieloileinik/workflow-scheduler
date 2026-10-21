namespace WorkflowScheduler.Client;

public enum WorkflowStepStatus
{
    Pending,
    Running,
    Waiting,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Compensated
}