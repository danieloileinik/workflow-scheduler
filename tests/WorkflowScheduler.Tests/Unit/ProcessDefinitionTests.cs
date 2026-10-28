namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ProcessDefinitionTests
{
    [Fact]
    public void Constructor_CopiesStepsAndExposesAnUnmodifiableCollection()
    {
        // Arrange
        var id = Guid.NewGuid();
        var first = new StepDefinition(Guid.NewGuid(), id, "first", StepType.WaitEvent, """{"eventName":"First"}""");
        var second = new StepDefinition(
            Guid.NewGuid(),
            id,
            "second",
            StepType.WaitEvent,
            """{"eventName":"Second"}""",
            position: 1);
        List<StepDefinition> supplied = [first];
        var definition = new ProcessDefinition(id, "workflow", 1, supplied);

        // Act
        supplied.Add(second);
        var stored = definition.FindStep(first.Id);

        // Assert
        Assert.Same(first, stored);
        Assert.Null(definition.FindStep(second.Id));
        Assert.Single(definition.Steps);
        Assert.Throws<NotSupportedException>(() => ((ICollection<StepDefinition>)definition.Steps).Clear());
    }

    [Fact]
    public void GetFirstStep_WhenSuppliedOrderDiffersFromPositions_ReturnsLowestPosition()
    {
        // Arrange
        var id = Guid.NewGuid();
        var first = new StepDefinition(Guid.NewGuid(), id, "first", StepType.WaitEvent, """{"eventName":"First"}""");
        var second = new StepDefinition(
            Guid.NewGuid(),
            id,
            "second",
            StepType.WaitEvent,
            """{"eventName":"Second"}""",
            position: 1);
        var definition = new ProcessDefinition(id, "workflow", 1, [second, first]);

        // Act
        var result = definition.GetFirstStep();

        // Assert
        Assert.False(result.IsError);
        Assert.Same(first, result.Value);
        Assert.Equal([first, second], definition.Steps);
    }

    [Fact]
    public void GetFirstStep_WhenNoStepsDefined_ReturnsNoStepsError()
    {
        // Arrange
        var definition = new ProcessDefinition(Guid.NewGuid(), "empty", 1, []);

        // Act
        var result = definition.GetFirstStep();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessDefinition.NoSteps", result.FirstError.Code);
    }
}