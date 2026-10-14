using System.Globalization;
using System.Text.Json;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ConditionStepStrategyTests
{
    [Theory]
    [InlineData("eq", "12.5", "12.5", true)]
    [InlineData("ne", "12.5", "10", true)]
    [InlineData("gt", "12.5", "12.4", true)]
    [InlineData("lt", "12.5", "13", true)]
    [InlineData("gte", "12.5", "12.5", true)]
    [InlineData("lte", "12.5", "12.5", true)]
    [InlineData("gt", "12.5", "13", false)]
    [InlineData("gt", "not-number", "13", false)]
    public async Task ExecuteAsync_WithRussianCulture_SelectsExpectedBranch(
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
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ru-RU");

        try
        {
            // Act
            var result = await strategy.ExecuteAsync(
                process,
                step,
                definition,
                CancellationToken.None);

            // Assert
            Assert.False(result.ShouldWait);
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
    public async Task ExecuteAsync_WithMissingPropertyOrNonObject_SelectsFalseBranch(string input)
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

        // Act
        var result = await strategy.ExecuteAsync(
            process,
            step,
            definition,
            CancellationToken.None);

        // Assert
        Assert.False(result.ShouldWait);
        Assert.Equal(falseStepId, result.OverrideNextStepId);
    }
}