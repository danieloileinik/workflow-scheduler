using Microsoft.Extensions.Caching.Memory;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Features.Processes;

namespace WorkflowScheduler.Infrastructure.Caching;

public sealed class PreparedProcessDefinitionCache : IDisposable
{
    private readonly MemoryCache _definitions;
    private readonly SemaphoreSlim[] _loadLocks = [.. Enumerable.Range(0, 64).Select(_ => new SemaphoreSlim(1, 1))];

    public PreparedProcessDefinitionCache(long capacityBytes = 32 * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacityBytes);
        _definitions = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacityBytes });
    }

    public async Task<PreparedProcessDefinition?> GetOrLoadAsync(
        Guid definitionId,
        Func<CancellationToken, Task<ProcessDefinition?>> load,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (_definitions.Get<PreparedProcessDefinition>(definitionId) is { } cached) return cached;

        SemaphoreSlim loadLock = _loadLocks[definitionId.GetHashCode() & (_loadLocks.Length - 1)];
        await loadLock.WaitAsync(ct);
        try
        {
            if (_definitions.Get<PreparedProcessDefinition>(definitionId) is { } loaded) return loaded;

            ProcessDefinition? definition = await load(ct);
            if (definition is null) return null;

            var prepared = new PreparedProcessDefinition(definition);
            // Approximate retained strings and per-step objects; oversized definitions are not cached.
            long size = 512 + definition.Steps.Sum(step => 256L + 2L * (step.Name.Length + step.Metadata.Length));
            _definitions.Set(definitionId, prepared, new MemoryCacheEntryOptions
            {
                Size = size,
                SlidingExpiration = TimeSpan.FromMinutes(30)
            });
            return prepared;
        }
        finally
        {
            loadLock.Release();
        }
    }

    public void Clear()
    {
        _definitions.Clear();
    }

    public void Dispose()
    {
        _definitions.Dispose();
        foreach (SemaphoreSlim loadLock in _loadLocks) loadLock.Dispose();
    }
}
