using WorkflowScheduler.Features.Steps.Strategies;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StepExecutorTests
{
    [Theory]
    [InlineData(StepType.Call, StepExecutionState.Waiting, StepStatus.Waiting, ProcessStatus.Waiting)]
    [InlineData(StepType.WaitEvent, StepExecutionState.Running, StepStatus.Running, ProcessStatus.Waiting)]
    [InlineData(StepType.Call, StepExecutionState.Completed, StepStatus.Completed, ProcessStatus.Completed)]
    public async Task RunAsync_UsesStrategyOutcomeInsteadOfStepType(StepType type,
        StepExecutionState state,
        StepStatus expectedStepStatus,
        ProcessStatus expectedProcessStatus)
    {
        var definition = new ProcessDefinition("custom-step", 1);
        var stepDefinition = new StepDefinition(definition.Id, "custom", type, "{}");
        definition.AddStep(stepDefinition);
        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(type, state);
        var executor = new StepExecutor([strategy]);

        var result = await executor.RunAsync(definition, process, stepDefinition, step, CancellationToken.None);

        Assert.False(result.IsError);
        Assert.Equal(expectedStepStatus, step.Status);
        Assert.Equal(expectedProcessStatus, process.Status);
        Assert.Equal(1, strategy.Executions);
        Assert.Single(process.Steps);
    }

    [Fact]
    public async Task RunAsync_WithCompletedStep_RejectsRestartBeforeExecutingStrategy()
    {
        var definition = new ProcessDefinition("completed-step", 1);
        var stepDefinition = new StepDefinition(definition.Id, "call", StepType.Call, "{}");
        definition.AddStep(stepDefinition);
        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        Assert.False(step.MarkRunning().IsError);
        Assert.False(step.Complete("saved output").IsError);
        var completedAt = step.CompletedAt;
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);

        var result = await executor.RunAsync(definition, process, stepDefinition, step, CancellationToken.None);

        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidTransition", result.FirstError.Code);
        Assert.Equal(0, strategy.Executions);
        Assert.Equal(StepStatus.Completed, step.Status);
        Assert.Equal("saved output", step.OutputData);
        Assert.Equal(completedAt, step.CompletedAt);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public async Task RunAsync_WithCompensation_KeepsProcessTerminalWhileExecutionRuns()
    {
        var definition = new ProcessDefinition("compensation", 1);
        var stepDefinition = new StepDefinition(definition.Id, "undo", StepType.Call, "{}");
        definition.AddStep(stepDefinition);
        var process = new ProcessInstance(definition.Id, "order", "{}");
        Assert.False(process.Cancel().IsError);
        var step = process.AddCompensationStep(stepDefinition.Id);
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);

        var result = await executor.RunAsync(definition, process, stepDefinition, step, CancellationToken.None);

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Equal(ProcessStatus.Cancelled, process.Status);
        Assert.Null(step.CompletedAt);
    }

    [Fact]
    public async Task RunAsync_WithUnknownStrategyOutcome_ReturnsError()
    {
        var definition = new ProcessDefinition("invalid-outcome", 1);
        var stepDefinition = new StepDefinition(definition.Id, "call", StepType.Call, "{}");
        definition.AddStep(stepDefinition);
        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(StepType.Call, (StepExecutionState)(-1));
        var executor = new StepExecutor([strategy]);

        var result = await executor.RunAsync(definition, process, stepDefinition, step, CancellationToken.None);

        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidExecutionState", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public async Task RunAsync_WithAutomaticCycle_ReturnsCycleErrorWithoutRecursion()
    {
        // Arrange
        var definition = new ProcessDefinition("cycle", 1);
        var stepId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            stepId,
            definition.Id,
            "loop",
            StepType.Condition,
            "{}",
            stepId,
            null);
        definition.AddStep(stepDefinition);
        var process = new ProcessInstance(definition.Id, "order", "{}");
        var start = process.Start(stepId);
        var strategy = new ContinueStrategy(stepId);
        var executor = new StepExecutor([strategy]);

        // Act
        var result = await executor.RunAsync(
            definition,
            process,
            stepDefinition,
            start.Value,
            CancellationToken.None);

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessDefinition.Cycle", result.FirstError.Code);
        Assert.Equal(2, process.Steps.Count);
    }

    private sealed class ContinueStrategy(Guid nextStepId) : IStepStrategy
    {
        public StepType StepType => StepType.Condition;

        public ValueTask<StepExecutionResult> ExecuteAsync(
            ProcessInstance processInstance,
            StepInstance stepInstance,
            StepDefinition stepDefinition,
            CancellationToken ct)
        {
            return ValueTask.FromResult(new StepExecutionResult(StepExecutionState.Completed, nextStepId));
        }
    }

    private sealed class ResultStrategy(StepType type, StepExecutionState state) : IStepStrategy
    {
        public int Executions { get; private set; }
        public StepType StepType => type;

        public ValueTask<StepExecutionResult> ExecuteAsync(ProcessInstance processInstance,
            StepInstance stepInstance,
            StepDefinition stepDefinition,
            CancellationToken ct)
        {
            Executions++;
            return ValueTask.FromResult(new StepExecutionResult(state));
        }
    }
}