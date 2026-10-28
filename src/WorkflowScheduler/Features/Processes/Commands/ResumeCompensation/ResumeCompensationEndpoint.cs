using MediatR;
using WorkflowScheduler.Common;
using GrpcServiceBase = WorkflowScheduler.Grpc.ResumeCompensation.ResumeCompensationBase;
using GrpcRequest = WorkflowScheduler.Grpc.ResumeCompensationRequest;
using GrpcResponse = WorkflowScheduler.Grpc.ResumeCompensationResponse;

namespace WorkflowScheduler.Features.Processes.Commands.ResumeCompensation;

public sealed class ResumeCompensationEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> ResumeCompensation(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var processId)
            || !Guid.TryParse(request.FailedStepInstanceId, out var stepId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process or failed step instance id"));

        var result = await mediator.Send(new ResumeCompensationCommand(processId, stepId), context.CancellationToken);

        return result.IsError ? throw result.Errors.ToRpcException() : new GrpcResponse();
    }
}
