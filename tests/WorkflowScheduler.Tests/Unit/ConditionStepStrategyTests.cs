using System.Globalization;
using System.Text.Json;
using WorkflowScheduler.Features.Steps.Strategies;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ConditionStepStrategyTests
{
    [Fact]
    public void ReusedStrategy_UsesEachProcessesInputWithoutReturningAStaleBranch()
    {
        // Arrange
        var yes = Guid.NewGuid();
        var no = Guid.NewGuid();
        var definition = new StepDefinition(
            Guid.NewGuid(),
            "condition",
            StepType.Condition,
            JsonSerializer.Serialize(
                new
                {
                    inputKey = "amount", @operator = "eq", value = "yes",
                    trueNextStepId = yes, falseNextStepId = no
                }));
        var strategy = new ConditionStepStrategy();
        var prepared = new PreparedStepDefinition(definition);
        foreach (var (input, expected) in new[] { ("yes", yes), ("yes", yes), ("no", no), ("yes", yes) })
        {
            var process = new ProcessInstance(
                definition.ProcessDefinitionId,
                "order",
                JsonSerializer.Serialize(new { amount = input }));
            var step = new StepInstance(process.Id, definition.Id);

            // Act
            var result = strategy.Execute(process, step, prepared);

            // Assert
            Assert.Equal(expected, result.OverrideNextStepId);
        }
    }

    [Theory]
    [InlineData("eq", "12.5", "12.5", true)]
    [InlineData("ne", "12.5", "10", true)]
    [InlineData("gt", "12.5", "12.4", true)]
    [InlineData("lt", "12.5", "13", true)]
    [InlineData("gte", "12.5", "12.5", true)]
    [InlineData("lte", "12.5", "12.5", true)]
    [InlineData("gt", "12.5", "13", false)]
    [InlineData("gt", "not-number", "13", false)]
    public void Execute_WithRussianCulture_SelectsExpectedBranch(
        string comparisonOperator,
        string input,
        string value,
        bool expected)
    {
        // Arrange
        var previousCulture = CultureInfo.CurrentCulture;
        var trueStepId = Guid.NewGuid();
        var falseStepId = Guid.NewGuid();
        var process = new ProcessInstance(
            Guid.NewGuid(),
            "order",
            JsonSerializer.Serialize(new { amount = input }));
        var step = new StepInstance(process.Id, Guid.NewGuid());
        var definition = new StepDefinition(
            process.DefinitionId,
            "condition",
            StepType.Condition,
            JsonSerializer.Serialize(
                new
                {
                    inputKey = "amount",
                    @operator = comparisonOperator,
                    value,
                    trueNextStepId = trueStepId,
                    falseNextStepId = falseStepId
                }));
        var strategy = new ConditionStepStrategy();
        var prepared = new PreparedStepDefinition(definition);
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ru-RU");

        try
        {
            // Act
            var result = strategy.Execute(
                process,
                step,
                prepared);

            // Assert
            Assert.Equal(StepExecutionState.Completed, result.State);
            Assert.Equal(expected ? trueStepId : falseStepId, result.OverrideNextStepId);
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
        }
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    public void Execute_WithMissingPropertyOrNonObject_SelectsFalseBranch(string input)
    {
        // Arrange
        var falseStepId = Guid.NewGuid();
        var process = new ProcessInstance(Guid.NewGuid(), "order", input);
        var step = new StepInstance(process.Id, Guid.NewGuid());
        var definition = new StepDefinition(
            process.DefinitionId,
            "condition",
            StepType.Condition,
            JsonSerializer.Serialize(
                new
                {
                    InputKey = "missing",
                    Operator = "eq",
                    Value = "x",
                    TrueNextStepId = Guid.NewGuid(),
                    FalseNextStepId = falseStepId
                }));
        var strategy = new ConditionStepStrategy();
        var prepared = new PreparedStepDefinition(definition);

        // Act
        var result = strategy.Execute(
            process,
            step,
            prepared);

        // Assert
        Assert.Equal(StepExecutionState.Completed, result.State);
        Assert.Equal(falseStepId, result.OverrideNextStepId);
    }
}
