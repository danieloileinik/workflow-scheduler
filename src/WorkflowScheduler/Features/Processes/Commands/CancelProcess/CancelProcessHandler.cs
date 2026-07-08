using ErrorOr;
using MediatR;
using Microsoft.EntityFrameworkCore;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public class CancelProcessHandler(AppDbContext db, IProcessDefinitionCache cache)
    : IRequestHandler<CancelProcessCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(CancelProcessCommand request, CancellationToken ct)
    {
        var instance = await db
            .ProcessInstances
            .Include(i => i.Steps)
            .FirstOrDefaultAsync(i => i.Id == request.ProcessInstanceId, ct);

        if (instance is null) return ProcessErrors.NotFound;

        if (instance.Status is ProcessStatus.Completed or ProcessStatus.Cancelled)
            return ProcessErrors.AlreadyCompleted;

        var definition = await GetDefinitionAsync(instance.DefinitionId, ct);

        instance.Cancel();

        if (definition is null) return Result.Success;
        foreach (var step in instance.GetCompletedSteps().Reverse())
        {
            var stepDef = definition.FindStep(step.StepDefinitionId);
            if (stepDef?.CompensationStepId is null) continue;
            db.SagaCompensationLogs.Add(new SagaCompensationLog(instance.Id, stepDef.Id));
        }

        return Result.Success;
    }

    private async Task<ProcessDefinition?> GetDefinitionAsync(Guid id, CancellationToken ct)
    {
        var cached = await cache.GetAsync(id, ct);
        if (cached is not null) return cached;

        return await db
            .ProcessDefinitions
            .Include(d => d.Steps)
            .FirstOrDefaultAsync(d => d.Id == id, ct);
    }
}