using MediatR;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;

public class GetProcessStatusHandler(AppDbContext db)
    : IRequestHandler<GetProcessStatusQuery, GetProcessStatusResponse?>
{
    public Task<GetProcessStatusResponse?> Handle(GetProcessStatusQuery request,
        CancellationToken ct)
    {
        return db
            .ProcessInstances
            .Where(i => i.Id == request.ProcessInstanceId)
            .Select(i => new GetProcessStatusResponse(
                i.Id,
                i.DefinitionId,
                i.BusinessKey,
                i.Status.ToString(),
                i.CurrentStepId,
                i.CreatedAt,
                i.UpdatedAt,
                i
                    .Steps
                    .OrderBy(s => s.Sequence)
                    .Skip(request.HistoryOffset)
                    .Take(request.HistoryPageSize == 0 ? int.MaxValue : request.HistoryPageSize)
                    .Select(s => new StepHistoryDto(
                        s.Id,
                        s.StepDefinitionId,
                        s.Status.ToString(),
                        s.StartedAt,
                        s.CompletedAt,
                        request.OmitOutputData ? null : s.OutputData,
                        s.Error,
                        s.RetryCount))
                    .ToList(),
                i.Steps.Count,
                request.HistoryOffset))
            .FirstOrDefaultAsync(ct);
    }
}