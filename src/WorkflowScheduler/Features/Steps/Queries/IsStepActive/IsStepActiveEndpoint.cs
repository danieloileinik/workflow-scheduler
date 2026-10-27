using MediatR;
using GrpcServiceBase = WorkflowScheduler.Grpc.IsStepActive.IsStepActiveBase;
using GrpcRequest = WorkflowScheduler.Grpc.IsStepActiveRequest;
using GrpcResponse = WorkflowScheduler.Grpc.IsStepActiveResponse;

namespace WorkflowScheduler.Features.Steps.Queries.IsStepActive;

public sealed class IsStepActiveEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> IsStepActive(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var processId)
            || processId == Guid.Empty
            || !Guid.TryParse(request.StepDefinitionId, out var definitionId)
            || definitionId == Guid.Empty
            || !Guid.TryParse(request.StepInstanceId, out var stepId)
            || stepId == Guid.Empty
            || request.Attempt < 0)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid step execution identity"));

        var active = await mediator.Send(
            new IsStepActiveQuery(
                processId,
                definitionId,
                stepId,
                request.Attempt,
                request.IsCompensation),
            context.CancellationToken);

        return new GrpcResponse { Active = active };
    }
}
