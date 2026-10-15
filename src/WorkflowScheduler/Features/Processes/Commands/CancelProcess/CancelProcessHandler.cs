using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public class CancelProcessHandler(AppDbContext db, CachedProcessDefinitionProvider definitions, SagaCoordinator saga)
    : IRequestHandler<CancelProcessCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(CancelProcessCommand request, CancellationToken ct)
    {
        var instance = await db
            .ProcessInstances
            .Include(x => x.Steps)
            .FirstOrDefaultAsync(x => x.Id == request.ProcessInstanceId, ct);
        if (instance is null) return ProcessErrors.NotFound;
        var result = instance.Cancel();
        if (result.IsError) return result;
        foreach (var step in instance.Steps.Where(x => !x.IsCompensation
                                                       && x.Status is StepStatus.Pending or StepStatus.Running
                                                           or StepStatus.Waiting))
        {
            var cancel = step.Cancel();
            if (cancel.IsError) return cancel;
        }

        var definition = await definitions.GetOrLoadAsync(instance.DefinitionId, ct);
        if (definition is null) return ProcessErrors.DefinitionNotFound;
        return await saga.ScheduleAsync(instance, definition, ct);
    }
}