using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using WorkflowScheduler.Contracts.Grpc;

namespace WorkflowScheduler.Infrastructure.Messaging;

public sealed class KafkaPublisher(IOptions<KafkaOptions> options) : IMessagePublisher, IAsyncDisposable
{
    private readonly IProducer<string, string> _producer = new ProducerBuilder<string, string>(
        options.Value.CreateProducerConfig()).Build();

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }

    public Task PublishAsync(string topic, string key, string value, CancellationToken ct)
    {
        return Encoding.UTF8.GetByteCount(value) > WorkflowProtocol.MaxKafkaValueBytes
            ? throw new ArgumentException("Command exceeds the Kafka message size limit.", nameof(value))
            : _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value }, ct);
    }
}