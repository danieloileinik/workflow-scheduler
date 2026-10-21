using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Features.Processes;

namespace WorkflowScheduler.Infrastructure.Caching;

public sealed class DisabledProcessDefinitionCache : IProcessDefinitionCache
{
    public Task<ProcessDefinition?> GetAsync(Guid definitionId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<ProcessDefinition?>(null);
    }

    public Task SetAsync(ProcessDefinition definition, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }
}