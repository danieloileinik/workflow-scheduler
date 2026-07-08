namespace WorkflowScheduler.Contracts.Events;

public sealed record StepCompletedEvent(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    Guid StepInstanceId,
    bool Success,
    string? OutputData,
    string? Error);