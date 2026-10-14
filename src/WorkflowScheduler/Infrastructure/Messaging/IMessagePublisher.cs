namespace WorkflowScheduler.Infrastructure.Messaging;

public interface IMessagePublisher
{
    Task PublishAsync(string topic, string key, string value, CancellationToken ct);
}