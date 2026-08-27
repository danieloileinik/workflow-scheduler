namespace WorkflowScheduler.Domain.Steps;

public class StepInstance(
    Guid id,
    Guid processInstanceId,
    Guid stepDefinitionId,
    int maxRetries,
    TimeSpan? timeout,
    int retryCount)
{
    public StepInstance(
        Guid processInstanceId,
        Guid stepDefinitionId,
        int retryCount = 0,
        int maxRetries = 3,
        TimeSpan? timeout = null)
        : this(Guid.NewGuid(), processInstanceId, stepDefinitionId, maxRetries, timeout, retryCount)
    {
    }

    public Guid Id { get; init; } = id;
    public int RetryCount { get; private set; } = retryCount;
    public DateTime? TimeoutAt { get; } = timeout.HasValue ? DateTime.UtcNow + timeout.Value : null;

    public int MaxRetries { get; } = maxRetries;

    public Guid ProcessInstanceId { get; init; } = processInstanceId;
    public Guid StepDefinitionId { get; init; } = stepDefinitionId;
    public StepStatus Status { get; private set; } = StepStatus.Pending;
    public DateTime StartedAt { get; init; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; private set; }
    public DateTime? NextRetryAt { get; private set; }
    public string? OutputData { get; private set; }
    public string? Error { get; private set; }

    public void MarkRunning()
    {
        Status = StepStatus.Running;
    }

    public void MarkWaiting()
    {
        Status = StepStatus.Waiting;
    }

    public void Complete(string? outputData = null)
    {
        Status = StepStatus.Completed;
        OutputData = outputData;
        CompletedAt = DateTime.UtcNow;
    }

    public bool TryScheduleRetry(TimeSpan delay)
    {
        if (RetryCount >= MaxRetries) return false;
        RetryCount++;
        Status = StepStatus.Pending;
        NextRetryAt = DateTime.UtcNow.Add(delay);
        Error = null;
        return true;
    }

    public void Fail(string error)
    {
        Status = StepStatus.Failed;
        Error = error;
        CompletedAt = DateTime.UtcNow;
    }

    public void MarkTimedOut()
    {
        Status = StepStatus.TimedOut;
        Error = "Step execution timed out";
        CompletedAt = DateTime.UtcNow;
    }

    public void MarkCompensated()
    {
        Status = StepStatus.Compensated;
    }

    public bool IsTimedOut()
    {
        return TimeoutAt.HasValue && DateTime.UtcNow > TimeoutAt.Value && Status == StepStatus.Running;
    }
}