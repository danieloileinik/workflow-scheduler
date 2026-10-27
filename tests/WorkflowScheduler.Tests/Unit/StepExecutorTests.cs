using WorkflowScheduler.Features.Processes;
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
        var definition = new ProcessDefinition("custom-step", 1);
        var stepDefinition = new StepDefinition(definition.Id, "custom", type, """{"serviceName":"test","command":"Run","eventName":"Event"}""");
        definition.AddStep(stepDefinition);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(type, state);
        var executor = new StepExecutor([strategy]);
        var prepared = new PreparedProcessDefinition(definition);

        // Act
        var result = executor.Run(prepared, process, prepared.FindStep(stepDefinition.Id)!, step, CancellationToken.None);

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
        var definition = new ProcessDefinition("completed-step", 1);
        var stepDefinition = new StepDefinition(definition.Id, "call", StepType.Call, """{"serviceName":"test","command":"Run"}""");
        definition.AddStep(stepDefinition);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        Assert.False(step.MarkRunning().IsError);
        Assert.False(step.Complete("saved output").IsError);

        var completedAt = step.CompletedAt;
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);
        var prepared = new PreparedProcessDefinition(definition);

        // Act
        var result = executor.Run(prepared, process, prepared.FindStep(stepDefinition.Id)!, step, CancellationToken.None);

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
        var definition = new ProcessDefinition("compensation", 1);
        var stepDefinition = new StepDefinition(definition.Id, "undo", StepType.Call, """{"serviceName":"test","command":"Run"}""");
        definition.AddStep(stepDefinition);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        Assert.False(process.Cancel().IsError);

        var step = process.AddCompensationStep(stepDefinition.Id);
        var strategy = new ResultStrategy(StepType.Call, StepExecutionState.Running);
        var executor = new StepExecutor([strategy]);
        var prepared = new PreparedProcessDefinition(definition);

        // Act
        var result = executor.Run(prepared, process, prepared.FindStep(stepDefinition.Id)!, step, CancellationToken.None);

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
        var definition = new ProcessDefinition("invalid-outcome", 1);
        var stepDefinition = new StepDefinition(definition.Id, "call", StepType.Call, """{"serviceName":"test","command":"Run"}""");
        definition.AddStep(stepDefinition);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var step = process.Start(stepDefinition.Id).Value;
        var strategy = new ResultStrategy(StepType.Call, (StepExecutionState)(-1));
        var executor = new StepExecutor([strategy]);
        var prepared = new PreparedProcessDefinition(definition);

        // Act
        var result = executor.Run(prepared, process, prepared.FindStep(stepDefinition.Id)!, step, CancellationToken.None);

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidExecutionState", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public void Run_WithAutomaticCycle_ReturnsCycleErrorWithoutRecursion()
    {
        // Arrange
        var definition = new ProcessDefinition("cycle", 1);
        var stepId = Guid.NewGuid();
        var stepDefinition = new StepDefinition(
            stepId,
            definition.Id,
            "loop",
            StepType.Condition,
            $$"""{"inputKey":"x","operator":"eq","value":"1","trueNextStepId":"{{stepId}}","falseNextStepId":"{{stepId}}"}""",
            stepId,
            null);
        definition.AddStep(stepDefinition);

        var process = new ProcessInstance(definition.Id, "order", "{}");
        var start = process.Start(stepId);
        var strategy = new ContinueStrategy(stepId);
        var executor = new StepExecutor([strategy]);
        var prepared = new PreparedProcessDefinition(definition);

        // Act
        var result = executor.Run(
            prepared,
            process,
            prepared.FindStep(stepDefinition.Id)!,
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

        public StepExecutionResult Execute(
            ProcessInstance processInstance,
            StepInstance stepInstance,
            PreparedStepDefinition stepDefinition)
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
            PreparedStepDefinition stepDefinition)
        {
            Executions++;

            return new StepExecutionResult(state);
        }
    }
}
