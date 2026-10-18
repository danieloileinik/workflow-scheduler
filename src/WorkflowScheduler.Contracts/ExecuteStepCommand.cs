namespace WorkflowScheduler.Contracts.Grpc;

public sealed record ExecuteStepCommand(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    Guid StepInstanceId,
    string Command,
    string? InputData,
    string CorrelationId,
    int Attempt = 0,
    bool IsCompensation = false);