using FluentValidation;
using GrpcRequest = WorkflowScheduler.Grpc.StartProcessRequest;
using GrpcResponse = WorkflowScheduler.Grpc.StartProcessResponse;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public class ProcessService
{
    public override async Task<GrpcResponse> StartProcess(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.DefinitionId, out var definitionId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid definition id"));

        try
        {
            var command = new StartProcessCommand(
                definitionId,
                request.BusinessKey,
                request.InputData,
                Guid.NewGuid());

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