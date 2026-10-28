using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Caching;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes;

public sealed class CachedProcessDefinitionProvider(
    AppDbContext db,
    IProcessDefinitionCache cache,
    ProcessDefinitionMemoryCache memoryCache)
{
    public Task<ProcessDefinition?> GetOrLoadAsync(Guid definitionId, CancellationToken ct)
    {
        return memoryCache.GetOrLoadAsync(definitionId, token => LoadAsync(definitionId, token), ct);
    }

    private async Task<ProcessDefinition?> LoadAsync(Guid definitionId, CancellationToken ct)
    {
        var cached = await cache.GetAsync(definitionId, ct);
        if (cached is not null)
            return cached;

        var definition = await db
            .ProcessDefinitions
            .AsNoTracking()
            .Include(candidate => candidate.Steps.OrderBy(step => step.Position))
            .FirstOrDefaultAsync(candidate => candidate.Id == definitionId, ct);

        if (definition is not null)
            await cache.SetAsync(definition, ct);

        return definition;
    }
}
