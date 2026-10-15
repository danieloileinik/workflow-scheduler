namespace WorkflowScheduler.Features.Steps.Strategies;

public readonly record struct StepExecutionResult(bool ShouldWait, Guid? OverrideNextStepId = null);