using MediatR;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessDefinition;

public class GetProcessDefinitionHandler(AppDbContext db) : IRequestHandler<GetProcessDefinitionQuery, Guid?>
{
    public Task<Guid?> Handle(GetProcessDefinitionQuery request, CancellationToken ct) =>
        db.ProcessDefinitions.Where(definition => definition.Name == request.Name && definition.Version == request.Version)
            .Select(definition => (Guid?)definition.Id).SingleOrDefaultAsync(ct);
}
