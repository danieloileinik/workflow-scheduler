using FluentValidation;
using MediatR;
using WorkflowScheduler.Common;
using GrpcServiceBase = WorkflowScheduler.Grpc.CancelProcessService.CancelProcessServiceBase;
using GrpcRequest = WorkflowScheduler.Grpc.CancelProcessRequest;
using GrpcResponse = WorkflowScheduler.Grpc.CancelProcessResponse;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public sealed class CancelProcessEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> CancelProcess(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        try
        {
            var command = new CancelProcessCommand(instanceId);
            var result = await mediator.Send(command, context.CancellationToken);
            return result.IsError ? throw result.Errors.ToRpcException() : new GrpcResponse();
        }
        catch (ValidationException ex)
        {
            throw ex.ToRpcException();
        }
    }
}