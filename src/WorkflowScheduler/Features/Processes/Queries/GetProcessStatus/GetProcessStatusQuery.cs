using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;

public record GetProcessStatusQuery(
    Guid ProcessInstanceId,
    int HistoryOffset = 0,
    int HistoryPageSize = 0,
    bool OmitOutputData = false)
    : IRequest<GetProcessStatusResponse?>, IQuery;

public record GetProcessStatusResponse(
    Guid Id,
    Guid DefinitionId,
    string BusinessKey,
    string Status,
    Guid? CurrentStepId,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<StepHistoryDto> StepHistory,
    int TotalStepCount = 0,
    int HistoryOffset = 0);

public record StepHistoryDto(
    Guid Id,
    Guid StepDefinitionId,
    string Status,
    DateTime StartedAt,
    DateTime? CompletedAt,
    string? OutputData,
    string? Error,
    int RetryCount);