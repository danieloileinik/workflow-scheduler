using MediatR;
using GrpcServiceBase = WorkflowScheduler.Grpc.GetProcessStatusService.GetProcessStatusServiceBase;
using GrpcRequest = WorkflowScheduler.Grpc.GetProcessStatusRequest;
using GrpcResponse = WorkflowScheduler.Grpc.GetProcessStatusResponse;
using GrpcStepHistoryEntry = WorkflowScheduler.Grpc.StepHistoryEntry;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;

public sealed class GetProcessStatusEndpoint(IMediator mediator) : GrpcServiceBase
{
    public override async Task<GrpcResponse> GetProcessStatus(GrpcRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.ProcessInstanceId, out var instanceId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid process instance id"));

        if (request.HistoryOffset < 0 || request.HistoryPageSize is < 0 or > 100
                                      || request is { HistoryOffset: > 0, HistoryPageSize: 0 })
            throw new RpcException(
                new Status(StatusCode.InvalidArgument, "History page size must be 1..100 with a nonnegative offset"));

        var query = new GetProcessStatusQuery(
            instanceId,
            request.HistoryOffset,
            request.HistoryPageSize,
            request.OmitOutputData);
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
            TotalStepCount = result.TotalStepCount,
            HistoryOffset = result.HistoryOffset,
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