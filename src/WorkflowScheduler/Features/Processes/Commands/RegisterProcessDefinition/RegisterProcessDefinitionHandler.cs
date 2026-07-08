using ErrorOr;
using MediatR;
using Microsoft.EntityFrameworkCore;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
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

        var processDefinition = new ProcessDefinition(request.Name, request.Version);

        foreach (var dto in request.Steps)
            processDefinition.AddStep(
                new StepDefinition(
                    dto.Id,
                    processDefinition.Id,
                    dto.Name,
                    dto.Type,
                    dto.Metadata,
                    dto.NextStepId,
                    dto.CompensationStepId));

        db.ProcessDefinitions.Add(processDefinition);

        return new RegisterProcessDefinitionResponse(processDefinition.Id);
    }
}