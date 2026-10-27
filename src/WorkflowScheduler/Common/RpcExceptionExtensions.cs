using ErrorOr;
using ValidationException = FluentValidation.ValidationException;

namespace WorkflowScheduler.Common;

public static class RpcExceptionExtensions
{
    public static RpcException ToRpcException(this List<Error> errors)
    {
        var error = errors[0];
        var statusCode = error.Type switch
        {
            ErrorType.NotFound => StatusCode.NotFound,
            ErrorType.Validation => StatusCode.InvalidArgument,
            ErrorType.Conflict => StatusCode.FailedPrecondition,
            ErrorType.Unauthorized or ErrorType.Forbidden => StatusCode.PermissionDenied,
            _ => StatusCode.Internal
        };

        return new RpcException(new Status(statusCode, error.Description));
    }

    public static RpcException ToRpcException(this ValidationException ex)
    {
        var description = string.Join("; ", ex.Errors.Select(e => e.ErrorMessage));

        return new RpcException(new Status(StatusCode.InvalidArgument, description));
    }
}