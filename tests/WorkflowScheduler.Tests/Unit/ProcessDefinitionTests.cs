namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ProcessDefinitionTests
{
    [Fact]
    public void FindStep_AfterAnotherStepIsAdded_UpdatesTheLookup()
    {
        var definition = new ProcessDefinition("workflow", 1);
        var first = new StepDefinition(definition.Id, "first", StepType.Call, "{}");
        definition.AddStep(first);
        Assert.Same(first, definition.FindStep(first.Id));
        var second = new StepDefinition(definition.Id, "second", StepType.Call, "{}");

        definition.AddStep(second);

        Assert.Same(first, definition.FindStep(first.Id));
        Assert.Same(second, definition.FindStep(second.Id));
        Assert.Null(definition.FindStep(Guid.NewGuid()));
    }
    [Fact]
    public void GetFirstStep_WhenCollectionOrderDiffersFromPositions_ReturnsLowestPosition()
    {
        // Arrange
        var definition = new ProcessDefinition("workflow", 1);
        var first = new StepDefinition(definition.Id, "first", StepType.Call, "{}");
        var second = new StepDefinition(definition.Id, "second", StepType.Call, "{}");
        definition.AddStep(second);
        definition.AddStep(first);
        first.SetPosition(0);
        second.SetPosition(1);

        // Act
        var result = definition.GetFirstStep();

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(first.Id, result.Value.Id);
    }

    [Fact]
    public void GetFirstStep_WhenNoStepsDefined_ReturnsNoStepsError()
    {
        // Arrange
        var definition = new ProcessDefinition("empty", 1);

        // Act
        var result = definition.GetFirstStep();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessDefinition.NoSteps", result.FirstError.Code);
    }
}