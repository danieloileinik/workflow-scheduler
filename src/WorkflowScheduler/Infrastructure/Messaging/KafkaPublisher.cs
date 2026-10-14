using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace WorkflowScheduler.Infrastructure.Messaging;

public sealed class KafkaPublisher(IOptions<KafkaOptions> options) : IMessagePublisher, IAsyncDisposable
{
    private readonly IProducer<string, string> _producer = new ProducerBuilder<string, string>(
        new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All
        }).Build();

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }

    public Task PublishAsync(string topic, string key, string value, CancellationToken ct)
    {
        return _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value }, ct);
    }
}