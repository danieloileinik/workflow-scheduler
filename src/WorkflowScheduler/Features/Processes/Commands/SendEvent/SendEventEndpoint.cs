using FluentValidation;
using MediatR;
using WorkflowScheduler.Common;
using GrpcServiceBase = WorkflowScheduler.Grpc.SendEvent.SendEventBase;
using GrpcRequest = WorkflowScheduler.Grpc.SendEventRequest;
using GrpcResponse = WorkflowScheduler.Grpc.SendEventResponse;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public sealed class SendEventEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> SendEvent(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        try
        {
            var command = new SendEventCommand(
                instanceId,
                request.EventName,
                string.IsNullOrEmpty(request.Payload) ? null : request.Payload,
                request.CorrelationId);

            var result = await mediator.Send(command, context.CancellationToken);
            return result.IsError ? throw result.Errors.ToRpcException() : new GrpcResponse();
        }
        catch (ValidationException ex)
        {
            throw ex.ToRpcException();
        }
    }
}