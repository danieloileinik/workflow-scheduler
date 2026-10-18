using FluentValidation;
using MediatR;
using WorkflowScheduler.Common;
using WorkflowScheduler.Domain.Steps;
using GrpcServiceBase = WorkflowScheduler.Grpc.RegisterProcessDefinitionService.RegisterProcessDefinitionServiceBase;
using GrpcRequest = WorkflowScheduler.Grpc.RegisterProcessDefinitionRequest;
using GrpcResponse = WorkflowScheduler.Grpc.RegisterProcessDefinitionResponse;
using GrpcStepTypeMessage = WorkflowScheduler.Grpc.StepTypeMessage;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public sealed class RegisterProcessDefinitionEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> RegisterProcessDefinition(GrpcRequest request, ServerCallContext context)
    {
        var steps = new List<StepDefinitionDto>(request.Steps.Count);

        foreach (var s in request.Steps)
        {
            if (!Guid.TryParse(s.Id, out var stepId))
                throw new RpcException(new Status(StatusCode.InvalidArgument, $"Invalid step id: {s.Id}"));

            steps.Add(
                new StepDefinitionDto(
                    stepId,
                    s.Name,
                    MapStepType(s.StepType),
                    s.Metadata,
                    ParseOptionalId(s.NextStepId),
                    ParseOptionalId(s.CompensationStepId)));
        }

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

    private static Guid? ParseOptionalId(string value)
    {
        if (string.IsNullOrEmpty(value)) return null;
        if (Guid.TryParse(value, out var id) && id != Guid.Empty) return id;
        throw new RpcException(new Status(StatusCode.InvalidArgument, $"Invalid step reference: {value}"));
    }

    private static StepType MapStepType(GrpcStepTypeMessage type)
    {
        return type switch
        {
            GrpcStepTypeMessage.Call => StepType.Call,
            GrpcStepTypeMessage.WaitEvent => StepType.WaitEvent,
            GrpcStepTypeMessage.Condition => StepType.Condition,
            _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"Unknown step type: {type}"))
        };
    }
}