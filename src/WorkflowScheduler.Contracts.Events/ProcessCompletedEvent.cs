namespace WorkflowScheduler.Contracts.Events;

public sealed record ProcessCompletedEvent(
    Guid ProcessInstanceId,
    Guid DefinitionId,
    string BusinessKey,
    DateTime CompletedAt);