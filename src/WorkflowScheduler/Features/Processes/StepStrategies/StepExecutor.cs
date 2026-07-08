using ErrorOr;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.StepStrategies;

public class StepExecutor(IEnumerable<IStepStrategy> strategies)
{
    public async Task<ErrorOr<Success>> ExecuteAsync(ProcessDefinition processDefinition,
        ProcessInstance processInstance,
        Guid nextStepDefId,
        CancellationToken ct)
    {
        var nexStepDef = processDefinition.FindStep(nextStepDefId);
        if (nexStepDef == null) return ProcessErrors.StepNotFound;

        var moveResult = processInstance.MoveToStep(nextStepDefId);
        if (moveResult.IsError) return moveResult.Errors;

        return await RunAsync(processDefinition, processInstance, nexStepDef, moveResult.Value, ct);
    }

    public async Task<ErrorOr<Success>> RunAsync(ProcessDefinition processDefinition,
        ProcessInstance processInstance,
        StepDefinition stepDefinition,
        StepInstance stepInstance,
        CancellationToken ct)
    {
        stepInstance.MarkRunning();

        var strategy = strategies.First(s => s.StepType == stepDefinition.Type);
        var result = await strategy.ExecuteAsync(processInstance, stepInstance, stepDefinition, ct);

        if (result.ShouldWait) return processInstance.Wait();

        stepInstance.Complete();

        var nextId = result.OverrideNextStepId ?? stepDefinition.NextStepId;
        return nextId is null
            ? processInstance.Complete()
            : await ExecuteAsync(
                processDefinition,
                processInstance,
                nextId.Value,
                ct);
    }
}