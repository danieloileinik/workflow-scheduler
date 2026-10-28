namespace WorkflowScheduler.Domain.Processes;

public class SagaCompensationLog
{
    private SagaCompensationLog()
    {
    }

    public SagaCompensationLog(Guid processInstanceId, Guid stepDefinitionId, Guid originalStepInstanceId, int sequence)
    {
        Id = Guid.NewGuid();
        ProcessInstanceId = processInstanceId;
        StepDefinitionId = stepDefinitionId;
        OriginalStepInstanceId = originalStepInstanceId;
        Sequence = sequence;
    }

    public Guid Id { get; private set; }
    public Guid ProcessInstanceId { get; private set; }
    public Guid StepDefinitionId { get; private set; }
    public Guid OriginalStepInstanceId { get; private set; }
    public Guid? StepInstanceId { get; private set; }
    public int Sequence { get; private set; }
    public CompensationStatus Status { get; private set; } = CompensationStatus.Pending;
    public DateTime StartedAt { get; private set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; private set; }
    public string? Error { get; private set; }

    public void MarkRunning(Guid stepInstanceId)
    {
        StepInstanceId = stepInstanceId;
        Status = CompensationStatus.Running;
        StartedAt = DateTime.UtcNow;
        CompletedAt = null;
        Error = null;
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
