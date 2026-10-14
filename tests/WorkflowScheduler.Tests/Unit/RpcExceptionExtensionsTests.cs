using Grpc.Core;
using WorkflowScheduler.Common;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class RpcExceptionExtensionsTests
{
    [Theory]
    [InlineData(ErrorType.NotFound, StatusCode.NotFound)]
    [InlineData(ErrorType.Validation, StatusCode.InvalidArgument)]
    [InlineData(ErrorType.Conflict, StatusCode.FailedPrecondition)]
    [InlineData(ErrorType.Unauthorized, StatusCode.PermissionDenied)]
    [InlineData(ErrorType.Forbidden, StatusCode.PermissionDenied)]
    [InlineData(ErrorType.Failure, StatusCode.Internal)]
    public void ToRpcException_WithDomainErrors_MapsStatusAndPreservesDescription(
        ErrorType errorType,
        StatusCode expectedStatus)
    {
        // Arrange
        var error = errorType switch
        {
            ErrorType.NotFound => Error.NotFound("test", "message"),
            ErrorType.Validation => Error.Validation("test", "message"),
            ErrorType.Conflict => Error.Conflict("test", "message"),
            ErrorType.Unauthorized => Error.Unauthorized("test", "message"),
            ErrorType.Forbidden => Error.Forbidden("test", "message"),
            _ => Error.Failure("test", "message")
        };
        var errors = new List<Error> { error };

        // Act
        var result = errors.ToRpcException();

        // Assert
        Assert.Equal(expectedStatus, result.StatusCode);
        Assert.Equal("message", result.Status.Detail);
    }
}