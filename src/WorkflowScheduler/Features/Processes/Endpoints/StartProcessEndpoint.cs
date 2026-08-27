using FluentValidation;
using WorkflowScheduler.Common;
using WorkflowScheduler.Features.Processes.Commands.StartProcess;
using GrpcRequest = WorkflowScheduler.Grpc.StartProcessRequest;
using GrpcResponse = WorkflowScheduler.Grpc.StartProcessResponse;

namespace WorkflowScheduler.Features.Processes.Endpoints;

public partial class ProcessService
{
    public override async Task<GrpcResponse> StartProcess(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.DefinitionId, out var definitionId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid definition id"));

        if (!Guid.TryParse(request.IdempotencyKey, out var idempotencyKey))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid idempotency key"));

        try
        {
            var command = new StartProcessCommand(definitionId, idempotencyKey, request.BusinessKey, request.InputData);

            var result = await mediator.Send(command, context.CancellationToken);
            return result.IsError
                ? throw result.Errors.ToRpcException()
                : new GrpcResponse { ProcessInstanceId = result.Value.ProcessInstanceId.ToString() };
        }
        catch (ValidationException ex)
        {
            throw ex.ToRpcException();
        }
    }
}