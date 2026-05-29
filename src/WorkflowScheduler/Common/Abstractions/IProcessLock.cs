namespace WorkflowScheduler.Common.Abstractions;

public interface IProcessLock
{
    Task<IAsyncDisposable?> TryAcquireAsync(Guid processInstanceId, CancellationToken ct = default);
}