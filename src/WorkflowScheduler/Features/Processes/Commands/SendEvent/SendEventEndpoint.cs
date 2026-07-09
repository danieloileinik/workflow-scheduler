using FluentValidation;
using MediatR;
using GrpcRequest = WorkflowScheduler.Grpc.SendEventRequest;
using GrpcResponse = WorkflowScheduler.Grpc.SendEventResponse;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public class ProcessService(IMediator mediator)
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