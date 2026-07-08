using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.StepStrategies;

public class WaitEventStepStrategy : IStepStrategy
{
    public StepType StepType => StepType.WaitEvent;

    public Task<StepExecutionResult> ExecuteAsync(ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        return Task.FromResult(new StepExecutionResult(true));
    }
}