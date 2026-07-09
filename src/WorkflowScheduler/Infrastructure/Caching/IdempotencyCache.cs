using StackExchange.Redis;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Infrastructure.Caching;

public class IdempotencyCache(IConnectionMultiplexer redis) : IIdempotencyCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(24);
    private IDatabase Db => redis.GetDatabase();

    public Task<bool> ExistsAsync(Guid idempotencyKey, CancellationToken ct = default)
    {
        return Db.KeyExistsAsync(Key(idempotencyKey));
    }

    public Task SaveAsync(Guid idempotencyKey, CancellationToken ct = default)
    {
        return Db.StringSetAsync(Key(idempotencyKey), 1, Ttl);
    }

    private static string Key(Guid key)
    {
        return $"idempotency:{key}";
    }
}