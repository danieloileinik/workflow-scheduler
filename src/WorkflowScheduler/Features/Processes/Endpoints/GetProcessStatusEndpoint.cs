using WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;
using GrpcRequest = WorkflowScheduler.Grpc.GetProcessStatusRequest;
using GrpcResponse = WorkflowScheduler.Grpc.GetProcessStatusResponse;
using GrpcStepHistoryEntry = WorkflowScheduler.Grpc.StepHistoryEntry;

namespace WorkflowScheduler.Features.Processes.Endpoints;

public partial class ProcessService
{
    public override async Task<GrpcResponse> GetProcessStatus(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        var query = new GetProcessStatusQuery(instanceId);
        var result = await mediator.Send(query, context.CancellationToken);

        if (result is null)
            throw new RpcException(new Status(StatusCode.NotFound, "Process instance not found"));

        return new GrpcResponse
        {
            Id = result.Id.ToString(),
            DefinitionId = result.DefinitionId.ToString(),
            BusinessKey = result.BusinessKey,
            Status = result.Status,
            CurrentStepId = result.CurrentStepId?.ToString() ?? string.Empty,
            CreatedAt = result.CreatedAt.ToString("O"),
            UpdatedAt = result.UpdatedAt.ToString("O"),
            StepHistory =
            {
                result.StepHistory.Select(s => new GrpcStepHistoryEntry
                {
                    Id = s.Id.ToString(),
                    StepDefinitionId = s.StepDefinitionId.ToString(),
                    Status = s.Status,
                    StartedAt = s.StartedAt.ToString("O"),
                    CompletedAt = s.CompletedAt?.ToString("O") ?? string.Empty,
                    OutputData = s.OutputData ?? string.Empty,
                    Error = s.Error ?? string.Empty,
                    RetryCount = s.RetryCount
                })
            }
        };
    }
}