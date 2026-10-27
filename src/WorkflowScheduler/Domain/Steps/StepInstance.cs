using ErrorOr;

namespace WorkflowScheduler.Domain.Steps;

public class StepInstance(
    Guid id,
    Guid processInstanceId,
    Guid stepDefinitionId,
    int maxRetries,
    int retryCount)
{
    public StepInstance(
        Guid processInstanceId,
        Guid stepDefinitionId,
        int retryCount = 0,
        int maxRetries = 3)
        : this(Guid.NewGuid(), processInstanceId, stepDefinitionId, maxRetries, retryCount)
    {
    }

    private StepInstance() : this(Guid.Empty, Guid.Empty, Guid.Empty, 3, 0)
    {
    }

    public int Sequence { get; private set; }
    public bool IsCompensation { get; private set; }

    public Guid Id { get; init; } = id;
    public int RetryCount { get; private set; } = retryCount;

    public int MaxRetries { get; } = maxRetries;

    public Guid ProcessInstanceId { get; init; } = processInstanceId;
    public Guid StepDefinitionId { get; init; } = stepDefinitionId;
    public StepStatus Status { get; private set; } = StepStatus.Pending;
    public DateTime StartedAt { get; init; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; private set; }
    public DateTime? NextRetryAt { get; private set; }
    public string? OutputData { get; private set; }
    public string? Error { get; private set; }

    public void SetSequence(int sequence, bool isCompensation = false)
    {
        Sequence = sequence;
        IsCompensation = isCompensation;
    }

    public ErrorOr<Success> MarkRunning()
    {
        if (Status != StepStatus.Pending) return StepErrors.InvalidTransition(Status, StepStatus.Running);
        Status = StepStatus.Running;
        NextRetryAt = null;
        return Result.Success;
    }

    public ErrorOr<Success> MarkWaiting()
    {
        if (Status != StepStatus.Running) return StepErrors.InvalidTransition(Status, StepStatus.Waiting);
        Status = StepStatus.Waiting;
        return Result.Success;
    }

    public ErrorOr<Success> Complete(string? outputData = null)
    {
        if (Status is not (StepStatus.Running or StepStatus.Waiting))
            return StepErrors.InvalidTransition(Status, StepStatus.Completed);
        Status = StepStatus.Completed;
        OutputData = outputData;
        CompletedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<bool> TryScheduleRetry(TimeSpan delay)
    {
        if (Status is not (StepStatus.Running or StepStatus.Failed or StepStatus.TimedOut))
            return StepErrors.InvalidTransition(Status, StepStatus.Pending);
        if (RetryCount >= MaxRetries) return false;
        RetryCount++;
        Status = StepStatus.Pending;
        NextRetryAt = DateTime.UtcNow.Add(delay);
        Error = null;
        CompletedAt = null;
        return true;
    }

    public ErrorOr<Success> Fail(string error)
    {
        if (Status is not (StepStatus.Running or StepStatus.Waiting))
            return StepErrors.InvalidTransition(Status, StepStatus.Failed);
        Status = StepStatus.Failed;
        Error = error;
        CompletedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<Success> Cancel()
    {
        if (Status is not (StepStatus.Pending or StepStatus.Running or StepStatus.Waiting))
            return StepErrors.InvalidTransition(Status, StepStatus.Cancelled);
        Status = StepStatus.Cancelled;
        NextRetryAt = null;
        CompletedAt = DateTime.UtcNow;
        return Result.Success;
    }
}