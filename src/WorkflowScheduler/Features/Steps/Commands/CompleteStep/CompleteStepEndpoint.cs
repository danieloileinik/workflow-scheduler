using ErrorOr;
using FluentValidation;
using MediatR;
using WorkflowScheduler.Common;
using GrpcServiceBase = WorkflowScheduler.Grpc.CompleteStepService.CompleteStepServiceBase;
using GrpcRequest = WorkflowScheduler.Grpc.CompleteStepRequest;
using GrpcResponse = WorkflowScheduler.Grpc.CompleteStepResponse;

namespace WorkflowScheduler.Features.Steps.Commands.CompleteStep;

public sealed class CompleteStepEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> CompleteStep(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        if (!Guid.TryParse(request.StepDefinitionId, out var stepDefinitionId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid step definition id"));

        if (!Guid.TryParse(request.StepInstanceId, out var stepInstanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid step instance id"));

        try
        {
            var command = new CompleteStepCommand(
                instanceId,
                stepDefinitionId,
                request.Success,
                string.IsNullOrEmpty(request.OutputData) ? null : request.OutputData,
                request.CorrelationId,
                stepInstanceId,
                request.Attempt);

            var result = await mediator.Send(command, context.CancellationToken);
            return result.IsError ? throw ((List<Error>)result.Errors).ToRpcException() : new GrpcResponse();
        }
        catch (ValidationException ex)
        {
            throw ex.ToRpcException();
        }
    }
}