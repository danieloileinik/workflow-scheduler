using Microsoft.Extensions.Caching.Memory;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Caching;

public sealed class ProcessDefinitionMemoryCache : IDisposable
{
    private readonly MemoryCache _definitions;
    private readonly SemaphoreSlim[] _loadLocks = [.. Enumerable.Range(0, 64).Select(_ => new SemaphoreSlim(1, 1))];

    public ProcessDefinitionMemoryCache(long capacityBytes = 32 * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacityBytes);
        _definitions = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacityBytes });
    }

    public void Dispose()
    {
        _definitions.Dispose();

        foreach (var loadLock in _loadLocks)
            loadLock.Dispose();
    }

    public async Task<ProcessDefinition?> GetOrLoadAsync(
        Guid definitionId,
        Func<CancellationToken, Task<ProcessDefinition?>> load,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (_definitions.Get<ProcessDefinition>(definitionId) is { } cached)
            return cached;

        var loadLock = _loadLocks[definitionId.GetHashCode() & (_loadLocks.Length - 1)];
        await loadLock.WaitAsync(ct);

        try
        {
            if (_definitions.Get<ProcessDefinition>(definitionId) is { } loaded)
                return loaded;

            var definition = await load(ct);
            if (definition is null)
                return null;

            definition.InitializeExecution();

            // Account for retained JSON, parsed strings and per-step objects; oversized definitions are not cached.
            long size = 1024 + 2L * definition.Name.Length
                            + definition.Steps.Sum(step => 384L + 2L * step.Name.Length + 4L * step.Metadata.Length);
            _definitions.Set(
                definitionId,
                definition,
                new MemoryCacheEntryOptions
                {
                    Size = size,
                    SlidingExpiration = TimeSpan.FromMinutes(30)
                });

            return definition;
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
}
