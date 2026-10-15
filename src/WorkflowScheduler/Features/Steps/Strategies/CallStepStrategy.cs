using System.Text.Json;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class CallStepStrategy(AppDbContext db) : IStepStrategy
{
    public StepType StepType => StepType.Call;

    public Task<StepExecutionResult> ExecuteAsync(
        ProcessInstance instance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        var metadata = JsonSerializer.Deserialize<CallMetadata>(stepDefinition.Metadata, StepMetadata.Options)!;

        var payload = JsonSerializer.Serialize(
            new StepCommandEvent(
                instance.Id,
                stepDefinition.Id,
                stepInstance.Id,
                metadata.Command,
                instance.InputData,
                stepInstance.Id.ToString(),
                stepInstance.RetryCount,
                stepInstance.IsCompensation));

        db.OutboxMessages.Add(
            new OutboxMessage
            {
                Type = $"process.commands.{metadata.ServiceName}",
                Payload = payload
            });

        return Task.FromResult(new StepExecutionResult(StepExecutionState.Running));
    }

    private record CallMetadata(string ServiceName, string Command);
}