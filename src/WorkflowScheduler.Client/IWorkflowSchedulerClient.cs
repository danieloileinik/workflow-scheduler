namespace WorkflowScheduler.Client;

public interface IWorkflowSchedulerClient
{
    Task<RegisteredWorkflow> RegisterAsync(WorkflowDefinition definition,
        CancellationToken cancellationToken = default);
    Task<RegisteredWorkflow> GetDefinitionAsync(string name, int version,
        CancellationToken cancellationToken = default);

    Task<Guid> StartAsync<TInput>(Guid definitionId,
        TInput input,
        string businessKey,
        Guid idempotencyKey,
        CancellationToken cancellationToken = default);

    Task<Guid> StartAsync<TInput>(RegisteredWorkflow definition,
        TInput input,
        string businessKey,
        Guid idempotencyKey,
        CancellationToken cancellationToken = default);

    Task SendEventAsync<TPayload>(Guid processInstanceId,
        string eventName,
        TPayload payload,
        string correlationId,
        CancellationToken cancellationToken = default);

    Task CompleteStepAsync<TOutput>(StepExecution execution,
        TOutput output,
        string correlationId,
        CancellationToken cancellationToken = default);

    Task FailStepAsync(StepExecution execution,
        string error,
        string correlationId,
        CancellationToken cancellationToken = default);

    Task CancelAsync(Guid processInstanceId, CancellationToken cancellationToken = default);
    Task<WorkflowProcessSnapshot> GetStatusAsync(Guid processInstanceId, CancellationToken cancellationToken = default);
    Task<bool> IsStepActiveAsync(StepExecution execution, bool isCompensation = false,
        CancellationToken cancellationToken = default);
}
