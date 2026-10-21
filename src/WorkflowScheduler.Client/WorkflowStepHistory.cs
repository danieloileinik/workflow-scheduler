using System.Text.Json;

namespace WorkflowScheduler.Client;

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