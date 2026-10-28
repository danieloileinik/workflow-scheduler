using WorkflowScheduler.Features.Steps.Strategies;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StepExecutorTests
{
    [Theory]
    [InlineData(StepType.Call, StepExecutionState.Waiting, StepStatus.Waiting, ProcessStatus.Waiting)]
    [InlineData(StepType.WaitEvent, StepExecutionState.Running, StepStatus.Running, ProcessStatus.Waiting)]
    [InlineData(StepType.Call, StepExecutionState.Completed, StepStatus.Completed, ProcessStatus.Completed)]
    public void Run_UsesStrategyOutcomeInsteadOfStepType(StepType type,
        StepExecutionState state,
        StepStatus expectedStepStatus,
        ProcessStatus expectedProcessStatus)
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            Guid.NewGuid(),
            definitionId,
            "custom",
            type,
            """{"serviceName":"test","command":"Run","eventName":"Event"}""");
        var definition = new ProcessDefinition(definitionId, "custom-step", 1, [stepDefinition]);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(type, state);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Run(
            definition,
            process,
            definition.FindStep(stepDefinition.Id)!,
            step,
            CancellationToken.None);

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(expectedStepStatus, step.Status);
        Assert.Equal(expectedProcessStatus, process.Status);
        Assert.Equal(1, strategy.Executions);
        Assert.Single(process.Steps);
    }

    [Fact]
    public void Run_WithCompletedStep_RejectsRestartBeforeExecutingStrategy()
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            Guid.NewGuid(),
            definitionId,
            "call",
            StepType.Call,
            """{"serviceName":"test","command":"Run"}""");
        var definition = new ProcessDefinition(definitionId, "completed-step", 1, [stepDefinition]);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        Assert.False(step.MarkRunning().IsError);
        Assert.False(step.Complete("saved output").IsError);

        var completedAt = step.CompletedAt;
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Run(
            definition,
            process,
            definition.FindStep(stepDefinition.Id)!,
            step,
            CancellationToken.None);

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidTransition", result.FirstError.Code);
        Assert.Equal(0, strategy.Executions);
        Assert.Equal(StepStatus.Completed, step.Status);
        Assert.Equal("saved output", step.OutputData);
        Assert.Equal(completedAt, step.CompletedAt);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public void Run_WithCompensation_KeepsProcessTerminalWhileExecutionRuns()
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            Guid.NewGuid(),
            definitionId,
            "undo",
            StepType.Call,
            """{"serviceName":"test","command":"Run"}""");
        var definition = new ProcessDefinition(definitionId, "compensation", 1, [stepDefinition]);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        Assert.False(process.Cancel().IsError);

        var step = process.AddCompensationStep(stepDefinition.Id);
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Run(
            definition,
            process,
            definition.FindStep(stepDefinition.Id)!,
            step,
            CancellationToken.None);

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Equal(ProcessStatus.Cancelled, process.Status);
        Assert.Null(step.CompletedAt);
    }

    [Fact]
    public void Run_WithUnknownStrategyOutcome_ReturnsError()
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            Guid.NewGuid(),
            definitionId,
            "call",
            StepType.Call,
            """{"serviceName":"test","command":"Run"}""");
        var definition = new ProcessDefinition(definitionId, "invalid-outcome", 1, [stepDefinition]);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(StepType.Call, (StepExecutionState)(-1));
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Run(
            definition,
            process,
            definition.FindStep(stepDefinition.Id)!,
            step,
            CancellationToken.None);

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidExecutionState", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public void Run_WithAutomaticCycle_ReturnsCycleErrorWithoutRecursion()
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var stepId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            stepId,
            definitionId,
            "loop",
            StepType.Condition,
            $$"""{"inputKey":"x","operator":"eq","value":"1","trueNextStepId":"{{stepId}}","falseNextStepId":"{{stepId}}"}""",
            stepId);
        var definition = new ProcessDefinition(definitionId, "cycle", 1, [stepDefinition]);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var start = process.Start(stepId);
        var strategy = new ContinueStrategy(stepId);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Run(
            definition,
            process,
            definition.FindStep(stepDefinition.Id)!,
            start.Value,
            CancellationToken.None);

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessDefinition.Cycle", result.FirstError.Code);
        Assert.Equal(2, process.Steps.Count);
    }

    [Fact]
    public void Redeliver_WithRunningCall_UsesRegisteredStrategyWithoutRestartingOrAdvancingAttempt()
    {
        // Arrange
        var definitionId = Guid.NewGuid();
        var definition = new StepDefinition(Guid.NewGuid(), definitionId, "call", StepType.Call,
            """{"serviceName":"test","command":"Run"}""");
        var process = new ProcessInstance(definitionId, "order", "{}");
        var step = process.Start(definition.Id).Value;
        Assert.False(step.MarkRunning().IsError);
        Assert.False(process.Wait().IsError);
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = executor.Redeliver(process, definition, step, CancellationToken.None);

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(1, strategy.Executions);
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Equal(0, step.RetryCount);
        Assert.Equal(ProcessStatus.Waiting, process.Status);
        Assert.Equal(step.Id, process.CurrentStepId);
        Assert.Same(step, Assert.Single(process.Steps));
    }

    private sealed class ContinueStrategy(Guid nextStepId) : IStepStrategy
    {
        public StepType StepType => StepType.Condition;

        public StepExecutionResult Execute(
            ProcessInstance processInstance,
            StepInstance stepInstance,
            StepDefinition stepDefinition)
        {
            return new StepExecutionResult(StepExecutionState.Completed, nextStepId);
        }
    }

    private sealed class ResultStrategy(StepType type, StepExecutionState state) : IStepStrategy
    {
        public int Executions { get; private set; }
        public StepType StepType => type;

        public StepExecutionResult Execute(ProcessInstance processInstance,
            StepInstance stepInstance,
            StepDefinition stepDefinition)
        {
            Executions++;

            return new StepExecutionResult(state);
        }
    }
}
