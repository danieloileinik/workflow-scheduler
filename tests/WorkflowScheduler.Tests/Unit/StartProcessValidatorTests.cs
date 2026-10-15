namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StartProcessValidatorTests
{
    [Fact]
    public void Validate_WithObjectInput_ReturnsValidResult()
    {
        // Arrange
        var validator = new StartProcessValidator();
        var command = new StartProcessCommand(Guid.NewGuid(), Guid.NewGuid(), "order", "{}");

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("null")]
    [InlineData("invalid")]
    public void Validate_WithNonObjectOrMalformedInput_ReturnsInputDataError(string input)
    {
        // Arrange
        var validator = new StartProcessValidator();
        var command = new StartProcessCommand(Guid.NewGuid(), Guid.NewGuid(), "order", input);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.PropertyName == nameof(StartProcessCommand.InputData));
    }

    [Theory]
    [InlineData('a', 1_000_000)]
    [InlineData('я', 180_000)]
    public void Validate_WithOversizedEscapedInput_ReturnsInputDataError(char character, int count)
    {
        var input = "{\"value\":\"" + new string(character, count) + "\"}";
        var command = new StartProcessCommand(Guid.NewGuid(), Guid.NewGuid(), "order", input);

        Assert.Contains(new StartProcessValidator().Validate(command).Errors,
            error => error.PropertyName == nameof(command.InputData));
    }
}
