using MediatR;
using GrpcServiceBase = WorkflowScheduler.Grpc.GetProcessDefinition.GetProcessDefinitionBase;
using GrpcRequest = WorkflowScheduler.Grpc.GetProcessDefinitionRequest;
using GrpcResponse = WorkflowScheduler.Grpc.GetProcessDefinitionResponse;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessDefinition;

public sealed class GetProcessDefinitionEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> GetProcessDefinition(GrpcRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || request.Name.Length > 256 || request.Version <= 0)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid definition name or version"));

        var id = await mediator.Send(
            new GetProcessDefinitionQuery(request.Name, request.Version),
            context.CancellationToken);

        return id is null
            ? throw new RpcException(new Status(StatusCode.NotFound, "Process definition not found"))
            : new GrpcResponse { DefinitionId = id.Value.ToString() };
    }
}