using FluentValidation;
using Grpc.Core.Interceptors;

namespace WorkflowScheduler.Common;

public sealed class ValidationExceptionInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (ValidationException exception)
        {
            throw exception.ToRpcException();
        }
    }
}