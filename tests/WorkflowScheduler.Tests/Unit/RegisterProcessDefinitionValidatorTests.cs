using System.Text.Json;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class RegisterProcessDefinitionValidatorTests
{
    [Theory]
    [InlineData("not-json")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("{\"command\":\"Execute\"}")]
    [InlineData("{\"serviceName\":\"billing\",\"command\":null}")]
    [InlineData("{\"serviceName\":123,\"ServiceName\":\"billing\",\"command\":\"Execute\"}")]
    [InlineData("{\"serviceName\":\"billing\",\"command\":\"Execute\",\"COMMAND\":\"Other\"}")]
    [InlineData("{\"serviceName\":\"billing v2\",\"command\":\"Execute\"}")]
    public void Validate_WithInvalidCallMetadata_ReturnsValidationError(string metadata)
    {
        // Arrange
        var validator = new RegisterProcessDefinitionValidator();
        var step = CreateCallStep() with { Metadata = metadata };
        var command = CreateDefinition(step);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.PropertyName == "Steps");
    }

    [Fact]
    public void Validate_WithDuplicateStepIds_ReturnsValidationError()
    {
        // Arrange
        var validator = new RegisterProcessDefinitionValidator();
        var step = CreateCallStep();
        var command = CreateDefinition(step, step);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.ErrorMessage == "Step ids must be unique");
    }

    [Fact]
    public void Validate_WithUnknownNextStep_ReturnsValidationError()
    {
        // Arrange
        var validator = new RegisterProcessDefinitionValidator();
        var step = CreateCallStep(Guid.NewGuid());
        var command = CreateDefinition(step);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(
            result.Errors,
            error =>
                error.ErrorMessage == "All step references must belong to this definition");
    }

    [Fact]
    public void Validate_WithConditionOnlyCycle_ReturnsValidationError()
    {
        // Arrange
        var validator = new RegisterProcessDefinitionValidator();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var first = CreateConditionStep(firstId, secondId);
        var second = CreateConditionStep(secondId, firstId);
        var command = CreateDefinition(first, second);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains(
            result.Errors,
            error =>
                error.ErrorMessage == "Automatic condition chains must be acyclic");
    }

    [Fact]
    public void Validate_WithLoopThroughWaitingStep_ReturnsValidResult()
    {
        // Arrange
        var validator = new RegisterProcessDefinitionValidator();
        var call = CreateCallStep();
        var wait = new StepDefinitionDto(
            Guid.NewGuid(),
            "Wait",
            StepType.WaitEvent,
            """{"eventName":"Approved"}""",
            call.Id,
            null);
        call = call with { NextStepId = wait.Id };
        var command = CreateDefinition(call, wait);

        // Act
        var result = validator.Validate(command);

        // Assert
        Assert.True(result.IsValid);
    }

    private static StepDefinitionDto CreateCallStep(Guid? nextStepId = null)
    {
        return new StepDefinitionDto(
            Guid.NewGuid(),
            "Call",
            StepType.Call,
            """{"serviceName":"billing","command":"Execute"}""",
            nextStepId,
            null);
    }

    [Theory]
    [InlineData("serviceName", 250)]
    [InlineData("command", 257)]
    [InlineData("eventName", 251)]
    public void Validate_WithUnusableMetadataLimits_ReturnsValidationError(string property, int length)
    {
        var metadata = new Dictionary<string, string> { ["serviceName"] = "billing", ["command"] = "Execute" };
        metadata[property] = new string('x', length);
        var step = CreateCallStep() with
        {
            Type = property == "eventName" ? StepType.WaitEvent : StepType.Call,
            Metadata = JsonSerializer.Serialize(metadata)
        };

        Assert.False(new RegisterProcessDefinitionValidator().Validate(CreateDefinition(step)).IsValid);
    }

    private static StepDefinitionDto CreateConditionStep(Guid id, Guid nextStepId)
    {
        return new StepDefinitionDto(
            id,
            "Condition",
            StepType.Condition,
            JsonSerializer.Serialize(
                new
                {
                    InputKey = "x",
                    Operator = "eq",
                    Value = "yes",
                    TrueNextStepId = nextStepId,
                    FalseNextStepId = nextStepId
                }),
            null,
            null);
    }

    private static RegisterProcessDefinitionCommand CreateDefinition(params StepDefinitionDto[] steps)
    {
        return new RegisterProcessDefinitionCommand("workflow", 1, steps);
    }
}