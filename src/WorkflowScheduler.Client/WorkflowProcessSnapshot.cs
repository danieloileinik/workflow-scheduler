namespace WorkflowScheduler.Client;

public sealed record WorkflowProcessSnapshot(
    Guid Id,
    Guid DefinitionId,
    string BusinessKey,
    WorkflowProcessStatus Status,
    Guid? CurrentStepId,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    IReadOnlyList<WorkflowStepHistory> StepHistory)
{
    public int TotalStepCount { get; init; }
    public int HistoryOffset { get; init; }
}