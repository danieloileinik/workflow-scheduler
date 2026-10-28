using System.Text.Json;
using WorkflowScheduler.Contracts.Grpc;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class CallStepStrategy(AppDbContext db) : IStepStrategy
{
    public StepType StepType => StepType.Call;

    public StepExecutionResult Execute(
        ProcessInstance instance,
        StepInstance stepInstance,
        PreparedStepDefinition stepDefinition)
    {
        var metadata = (StepMetadata.Call)stepDefinition.Metadata;

        var payload = JsonSerializer.Serialize(
            new ExecuteStepCommand(
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

        return new StepExecutionResult(StepExecutionState.Running);
    }
}