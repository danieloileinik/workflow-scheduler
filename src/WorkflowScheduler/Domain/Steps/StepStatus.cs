namespace WorkflowScheduler.Domain.Steps;

public enum StepStatus
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