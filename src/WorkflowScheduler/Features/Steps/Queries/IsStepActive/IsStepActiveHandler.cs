using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Steps.Queries.IsStepActive;

public class IsStepActiveHandler(AppDbContext db) : IRequestHandler<IsStepActiveQuery, bool>
{
    public Task<bool> Handle(IsStepActiveQuery request, CancellationToken ct) =>
        db.ProcessInstances.Where(process => process.Id == request.ProcessInstanceId
                && (request.IsCompensation
                    ? process.Status == ProcessStatus.Cancelled || process.Status == ProcessStatus.Failed
                    : process.Status == ProcessStatus.Waiting && process.CurrentStepId == request.StepInstanceId))
            .AnyAsync(process => db.StepInstances.Any(step => step.ProcessInstanceId == process.Id
                && step.Id == request.StepInstanceId && step.StepDefinitionId == request.StepDefinitionId
                && step.Status == StepStatus.Running && step.RetryCount == request.Attempt
                && step.IsCompensation == request.IsCompensation), ct);
}
