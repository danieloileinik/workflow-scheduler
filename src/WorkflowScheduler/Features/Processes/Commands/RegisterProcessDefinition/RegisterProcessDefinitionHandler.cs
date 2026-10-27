using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class RegisterProcessDefinitionHandler(AppDbContext db)
    : IRequestHandler<RegisterProcessDefinitionCommand, ErrorOr<RegisterProcessDefinitionResponse>>
{
    public async Task<ErrorOr<RegisterProcessDefinitionResponse>> Handle(
        RegisterProcessDefinitionCommand request,
        CancellationToken ct)
    {
        var exists = await db.ProcessDefinitions
            .AnyAsync(d => d.Name == request.Name && d.Version == request.Version, ct);

        if (exists) return ProcessErrors.DefinitionVersionConflict;

        var stepIds = request.Steps.Select(s => s.Id).ToArray();
        if (await db.StepDefinitions.AnyAsync(s => stepIds.AsEnumerable().Contains(s.Id), ct))
            return Error.Conflict(
                "ProcessDefinition.StepIdConflict",
                "Step ids are already used by another definition");
        db.ProcessDefinitions.Add(request.Definition);

        return new RegisterProcessDefinitionResponse(request.Definition.Id);
    }
}
