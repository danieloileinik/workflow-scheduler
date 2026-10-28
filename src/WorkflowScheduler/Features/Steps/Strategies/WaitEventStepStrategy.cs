using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class WaitEventStepStrategy : IStepStrategy
{
    public StepType StepType => StepType.WaitEvent;

    public StepExecutionResult Execute(ProcessInstance processInstance,
        StepInstance stepInstance,
        PreparedStepDefinition stepDefinition)
    {
        return new StepExecutionResult(StepExecutionState.Waiting);
    }
}