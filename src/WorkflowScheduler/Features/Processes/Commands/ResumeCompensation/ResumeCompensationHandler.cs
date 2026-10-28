using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.ResumeCompensation;

public sealed class ResumeCompensationHandler(
    AppDbContext db,
    CachedProcessDefinitionProvider definitions,
    SagaCoordinator saga)
    : IRequestHandler<ResumeCompensationCommand, ErrorOr<Success>>
{
    public async Task<ErrorOr<Success>> Handle(ResumeCompensationCommand request, CancellationToken ct)
    {
        var correlationId = request.FailedStepInstanceId.ToString("N");
        if (await db.EventLogs.AnyAsync(log => log.ProcessInstanceId == request.ProcessInstanceId
                                              && log.EventName == "ResumeCompensation"
                                              && log.CorrelationId == correlationId, ct))
            return Result.Success;

        var instance = await db.LoadActiveProcessAsync(request.ProcessInstanceId, ct);
        if (instance is null)
            return ProcessErrors.NotFound;

        if (instance.Status is not (ProcessStatus.Failed or ProcessStatus.Cancelled))
            return ProcessErrors.NotCompensating;

        var definition = await definitions.GetOrLoadAsync(instance.DefinitionId, ct);
        if (definition is null)
            return ProcessErrors.DefinitionNotFound;

        var result = await saga.ResumeAsync(instance, definition, request.FailedStepInstanceId, ct);
        if (result.IsError)
            return result;

        db.EventLogs.Add(new EventLog(instance.Id, "ResumeCompensation", correlationId));

        return Result.Success;
    }
}
