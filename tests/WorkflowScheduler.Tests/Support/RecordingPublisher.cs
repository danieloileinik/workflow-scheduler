using System.Collections.Concurrent;
using WorkflowScheduler.Infrastructure.Messaging;

namespace WorkflowScheduler.Tests.Support;

internal sealed class RecordingPublisher(IMessagePublisher inner) : IMessagePublisher
{
    private readonly ConcurrentQueue<(string Topic, string Key, string Payload)> _messages = new();
    public IReadOnlyList<(string Topic, string Key, string Payload)> Messages => [.. _messages];
    public bool Fail { get; set; }
    public Func<CancellationToken, Task>? BeforePublishAsync { get; set; }

    public async Task PublishAsync(string topic, string key, string value, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (Fail) throw new InvalidOperationException(new string('x', 3000));
        if (BeforePublishAsync is { } before) await before(ct);
        await inner.PublishAsync(topic, key, value, ct);
        _messages.Enqueue((topic, key, value));
    }

    public void Reset()
    {
        _messages.Clear();
        Fail = false;
        BeforePublishAsync = null;
    }
}