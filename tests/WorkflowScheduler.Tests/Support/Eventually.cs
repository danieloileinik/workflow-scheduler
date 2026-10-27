namespace WorkflowScheduler.Tests.Support;

internal static class Eventually
{
    public static async Task<T> UntilAsync<T>(Func<CancellationToken, Task<T>> read,
        Func<T, bool> matches,
        string description,
        TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(20);
        using var deadline = new CancellationTokenSource(limit);
        T? last = default;

        try
        {
            while (true)
            {
                last = await read(deadline.Token);
                if (matches(last))
                    return last;

                await Task.Delay(TimeSpan.FromMilliseconds(50), deadline.Token);
            }
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            Assert.Fail($"{description} within {limit}. Last observed value: {last}");
            throw;
        }
    }
}