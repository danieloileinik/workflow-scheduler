using FluentValidation;
using GrpcRequest = WorkflowScheduler.Grpc.CompleteStepRequest;
using GrpcResponse = WorkflowScheduler.Grpc.CompleteStepResponse;

namespace WorkflowScheduler.Features.Processes.Commands.CompleteStep;

public class ProcessService
{
    public override async Task<GrpcResponse> CompleteStep(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        if (!Guid.TryParse(request.StepDefinitionId, out var stepDefinitionId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid step definition id"));

        try
        {
            var command = new CompleteStepCommand(
                instanceId,
                stepDefinitionId,
                request.Success,
                string.IsNullOrEmpty(request.OutputData) ? null : request.OutputData,
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