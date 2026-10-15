using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class WaitEventStepStrategy : IStepStrategy
{
    public StepType StepType => StepType.WaitEvent;

    public ValueTask<StepExecutionResult> ExecuteAsync(ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        return ValueTask.FromResult(new StepExecutionResult(StepExecutionState.Waiting));
    }
}