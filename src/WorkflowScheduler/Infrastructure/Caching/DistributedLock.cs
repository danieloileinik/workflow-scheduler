using StackExchange.Redis;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Infrastructure.Caching;

public class DistributedLock(IConnectionMultiplexer redis) : IProcessLock
{
    private const string ReleaseScript = """
                                         if redis.call('get', KEYS[1]) == ARGV[1] then
                                             return redis.call('del', KEYS[1])
                                         end
                                         return 0
                                         """;

    private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(30);

    public async Task<IAsyncDisposable?> TryAcquireAsync(Guid processInstanceId, CancellationToken ct = default)
    {
        var db = redis.GetDatabase();
        var key = $"lock:process:{processInstanceId}";
        var token = Guid.NewGuid().ToString("N");

        var acquired = await db.StringSetAsync(key, token, LockTtl, When.NotExists);
        return acquired ? new LockHandle(db, key, token) : null;
    }

    private sealed class LockHandle(IDatabase db, string key, string token) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await db.ScriptEvaluateAsync(ReleaseScript, [(RedisKey)key], [(RedisValue)token]);
        }
    }
}