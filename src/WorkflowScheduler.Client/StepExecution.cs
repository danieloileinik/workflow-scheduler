namespace WorkflowScheduler.Client;

public sealed record StepExecution(Guid ProcessInstanceId, Guid StepDefinitionId, Guid StepInstanceId, int Attempt);