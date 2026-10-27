using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Caching;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes;

public sealed class CachedProcessDefinitionProvider(
    AppDbContext db,
    IProcessDefinitionCache cache,
    PreparedProcessDefinitionCache prepared)
{
    public Task<PreparedProcessDefinition?> GetOrLoadAsync(Guid definitionId, CancellationToken ct)
    {
        return prepared.GetOrLoadAsync(definitionId, token => LoadAsync(definitionId, token), ct);
    }

    private async Task<ProcessDefinition?> LoadAsync(Guid definitionId, CancellationToken ct)
    {
        ProcessDefinition? cached = await cache.GetAsync(definitionId, ct);
        if (cached is not null) return cached;

        ProcessDefinition? definition = await db.ProcessDefinitions
            .AsNoTracking()
            .Include(candidate => candidate.Steps)
            .FirstOrDefaultAsync(candidate => candidate.Id == definitionId, ct);

        if (definition is not null)
            await cache.SetAsync(definition, ct);

        return definition;
    }
}
