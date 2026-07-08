using FluentValidation;
using GrpcRequest = WorkflowScheduler.Grpc.RegisterProcessDefinitionRequest;
using GrpcResponse = WorkflowScheduler.Grpc.RegisterProcessDefinitionResponse;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class ProcessService
{
    public override async Task<GrpcResponse> RegisterProcessDefinition(
        GrpcRequest request,
        Grpc.Core.ServerCallContext context)
    {
        var steps = request
            .Steps
            .Select(s => new StepDefinitionDto(
                s.Id,
                s.Name,
                s.StepType,
                s.Metadata,
                string.IsNullOrEmpty(s.NextStepId) ? null : s.NextStepId,
                string.IsNullOrEmpty(s.CompensationStepId) ? null : s.CompensationStepId))
            .ToList();

        try
        {
            var command = new RegisterProcessDefinitionCommand(request.Name, request.Version, steps);
            var result = await mediator.Send(command, context.CancellationToken);
            return result.IsError
                ? throw result.Errors.ToRpcException()
                : new GrpcResponse { DefinitionId = result.Value.DefinitionId.ToString() };
        }
        catch (ValidationException ex)
        {
            throw ex.ToRpcException();
        }
    }
}