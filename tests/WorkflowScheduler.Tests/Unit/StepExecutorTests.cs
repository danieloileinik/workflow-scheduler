using WorkflowScheduler.Features.Steps.Strategies;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StepExecutorTests
{
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

        public Task<StepExecutionResult> ExecuteAsync(
            ProcessInstance processInstance,
            StepInstance stepInstance,
            StepDefinition stepDefinition,
            CancellationToken ct)
        {
            return Task.FromResult(new StepExecutionResult(false, nextStepId));
        }
    }
}