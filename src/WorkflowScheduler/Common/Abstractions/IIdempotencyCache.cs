namespace WorkflowScheduler.Common.Abstractions;

public interface IIdempotencyCache
{
    Task<bool> ExistsAsync(Guid idempotencyKey, CancellationToken ct = default);
    Task SaveAsync(Guid idempotencyKey, CancellationToken ct = default);
}