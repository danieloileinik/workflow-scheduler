using MediatR;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;

public class GetProcessStatusHandler(AppDbContext db)
    : IRequestHandler<GetProcessStatusQuery, GetProcessStatusResponse?>
{
    public async Task<GetProcessStatusResponse?> Handle(GetProcessStatusQuery request,
        CancellationToken ct)
    {
        return await db
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
                    .OrderBy(s => s.StartedAt)
                    .Select(s => new StepHistoryDto(
                        s.Id,
                        s.StepDefinitionId,
                        s.Status.ToString(),
                        s.StartedAt,
                        s.CompletedAt,
                        s.OutputData,
                        s.Error,
                        s.RetryCount))
                    .ToList()))
            .FirstOrDefaultAsync(ct);
    }
}