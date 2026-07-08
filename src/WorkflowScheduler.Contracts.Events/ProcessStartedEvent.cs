namespace WorkflowScheduler.Contracts.Events;

public sealed record ProcessStartedEvent(
    Guid ProcessInstanceId,
    Guid DefinitionId,
    string BusinessKey,
    DateTime StartedAt);