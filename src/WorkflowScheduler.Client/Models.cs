using System.Text.Json;

namespace WorkflowScheduler.Client;

public sealed record RegisteredWorkflow(Guid Id, string Name, int Version);

public enum WorkflowProcessStatus
{
    Created,
    Running,
    Waiting,
    Failed,
    Completed,
    Cancelled
}

public enum WorkflowStepStatus
{
    Pending,
    Running,
    Waiting,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Compensated
}

public sealed record StepExecution(Guid ProcessInstanceId, Guid StepDefinitionId, Guid StepInstanceId, int Attempt);

public sealed record WorkflowStepHistory(
    Guid Id,
    Guid StepDefinitionId,
    WorkflowStepStatus Status,
    DateTimeOffset StartedAt,
    DateTimeOffset? CompletedAt,
    string? OutputData,
    string? Error,
    int RetryCount)
{
    public StepExecution Execution(Guid processInstanceId)
    {
        return new StepExecution(processInstanceId, StepDefinitionId, Id, RetryCount);
    }

    public T? ReadOutput<T>(JsonSerializerOptions? options = null)
    {
        return OutputData is null
            ? default
            : JsonSerializer.Deserialize<T>(
                OutputData,
                options ?? JsonSerializerOptions.Web);
    }
}

public sealed record WorkflowProcessSnapshot(
    Guid Id,
    Guid DefinitionId,
    string BusinessKey,
    WorkflowProcessStatus Status,
    Guid? CurrentStepId,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    IReadOnlyList<WorkflowStepHistory> StepHistory);