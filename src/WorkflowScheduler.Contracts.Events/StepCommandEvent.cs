namespace WorkflowScheduler.Contracts.Events;

public sealed record StepCommandEvent(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    Guid StepInstanceId,
    string Command,
    string? InputData,
    string CorrelationId);