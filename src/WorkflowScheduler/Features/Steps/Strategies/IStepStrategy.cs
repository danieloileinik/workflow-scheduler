using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public interface IStepStrategy
{
    StepType StepType { get; }

    StepExecutionResult Execute(
        ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition);
}
