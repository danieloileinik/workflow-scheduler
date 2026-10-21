namespace WorkflowScheduler.Client;

public sealed record WorkflowStepDefinition(
    Guid Id,
    string Name,
    WorkflowStepKind Kind,
    string Metadata,
    Guid? NextStepId,
    Guid? CompensationStepId);