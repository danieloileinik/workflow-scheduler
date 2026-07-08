namespace WorkflowScheduler.Features.Processes.StepStrategies;

public readonly record struct StepExecutionResult(bool ShouldWait, Guid? OverrideNextStepId = null);