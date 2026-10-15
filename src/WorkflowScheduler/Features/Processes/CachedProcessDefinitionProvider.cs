using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes;

public sealed class CachedProcessDefinitionProvider(AppDbContext db, IProcessDefinitionCache cache)
{
    /// <summary>Gets a cached definition, or loads it from the database and caches it when found.</summary>
    public async Task<ProcessDefinition?> GetOrLoadAsync(Guid definitionId, CancellationToken ct)
    {
        var cached = await cache.GetAsync(definitionId, ct);
        if (cached is not null) return cached;

        var definition = await db
            .ProcessDefinitions
            .Include(candidate => candidate.Steps)
            .FirstOrDefaultAsync(candidate => candidate.Id == definitionId, ct);

        if (definition is not null)
            await cache.SetAsync(definition, ct);

        return definition;
    }
}