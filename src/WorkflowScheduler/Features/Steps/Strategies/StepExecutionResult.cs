namespace WorkflowScheduler.Features.Steps.Strategies;

public readonly record struct StepExecutionResult(StepExecutionState State, Guid? OverrideNextStepId = null);