using System.Text.Json;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.StepStrategies;

public class CallStepStrategy(AppDbContext db) : IStepStrategy
{
    public StepType StepType => StepType.Call;

    public Task<StepExecutionResult> ExecuteAsync(
        ProcessInstance instance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        var metadata = JsonSerializer.Deserialize<CallMetadata>(stepDefinition.Metadata)!;

        var payload = JsonSerializer.Serialize(
            new StepCommandMessage(
                instance.Id,
                stepDefinition.Id,
                stepInstance.Id,
                metadata.Command,
                instance.InputData,
                stepInstance.Id.ToString()));

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Type = $"process.commands.{metadata.ServiceName}",
                Payload = payload
            });

        return Task.FromResult(new StepExecutionResult(true));
    }

    private record CallMetadata(string ServiceName, string Command);

    private record StepCommandMessage(
        Guid ProcessInstanceId,
        Guid StepDefinitionId,
        Guid StepInstanceId,
        string Command,
        string? InputData,
        string CorrelationId);
}