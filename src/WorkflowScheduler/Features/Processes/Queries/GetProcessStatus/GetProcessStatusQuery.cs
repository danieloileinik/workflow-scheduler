using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;

public record GetProcessStatusQuery(Guid ProcessInstanceId)
    : IRequest<GetProcessStatusResponse?>, IQuery;

public record GetProcessStatusResponse(
    Guid Id,
    Guid DefinitionId,
    string BusinessKey,
    string Status,
    Guid? CurrentStepId,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<StepHistoryDto> StepHistory);

public record StepHistoryDto(
    Guid Id,
    Guid StepDefinitionId,
    string Status,
    DateTime StartedAt,
    DateTime? CompletedAt,
    string? OutputData,
    string? Error,
    int RetryCount);