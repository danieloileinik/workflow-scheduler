using WorkflowScheduler.Contracts.Grpc;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class CallbackOutputLimitTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CallbackOutputLimit_UsesUtf8BytesAndServerValidatorSharesBoundary(bool oversized)
    {
        // Arrange
        var output = new string('я', WorkflowProtocol.MaxOutputDataBytes / 2) + (oversized ? "x" : "");
        var command = new CompleteStepCommand(
            Guid.NewGuid(),
            Guid.NewGuid(),
            true,
            output,
            "callback",
            Guid.NewGuid());

        // Act
        var fits = WorkflowProtocol.OutputFitsMessage(output);
        var validation = new CompleteStepValidator().Validate(command);

        // Assert
        Assert.Equal(!oversized, fits);
        Assert.Equal(!oversized, validation.IsValid);
    }
}