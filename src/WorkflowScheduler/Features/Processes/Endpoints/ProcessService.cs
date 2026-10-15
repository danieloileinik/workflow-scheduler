using MediatR;
using WorkflowScheduler.Features.Steps.Endpoints;
using GrpcProcessesBase = WorkflowScheduler.Grpc.Processes.ProcessesBase;
using GrpcRequest = WorkflowScheduler.Grpc.CompleteStepRequest;
using GrpcResponse = WorkflowScheduler.Grpc.CompleteStepResponse;

namespace WorkflowScheduler.Features.Processes.Endpoints;

public partial class ProcessService(IMediator mediator) : GrpcProcessesBase
{
    public override Task<GrpcResponse> CompleteStep(GrpcRequest request, ServerCallContext context)
    {
        return CompleteStepEndpoint.HandleAsync(request, context, mediator);
    }
}