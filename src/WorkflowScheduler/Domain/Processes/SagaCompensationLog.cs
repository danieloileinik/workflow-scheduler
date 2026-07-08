namespace WorkflowScheduler.Domain.Processes;

public class SagaCompensationLog
{
    public SagaCompensationLog(Guid processInstanceId, Guid stepDefinitionId)
        : this(Guid.NewGuid(), processInstanceId, stepDefinitionId)
    {
    }

    private SagaCompensationLog(Guid id, Guid processInstanceId, Guid stepDefinitionId)
    {
        Id = id;
        ProcessInstanceId = processInstanceId;
        StepDefinitionId = stepDefinitionId;
        Status = CompensationStatus.Pending;
        StartedAt = DateTime.UtcNow;
    }

    public Guid Id { get; init; }
    public Guid ProcessInstanceId { get; init; }
    public Guid StepDefinitionId { get; init; }
    public CompensationStatus Status { get; private set; }
    public DateTime StartedAt { get; init; }
    public DateTime? CompletedAt { get; private set; }
    public string? Error { get; private set; }

    public void MarkRunning()
    {
        Status = CompensationStatus.Running;
    }

    public void Complete()
    {
        Status = CompensationStatus.Done;
        CompletedAt = DateTime.UtcNow;
    }

    public void Fail(string error)
    {
        Status = CompensationStatus.Failed;
        Error = error;
        CompletedAt = DateTime.UtcNow;
    }
}