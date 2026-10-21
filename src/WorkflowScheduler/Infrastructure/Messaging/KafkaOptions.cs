using Confluent.Kafka;

namespace WorkflowScheduler.Infrastructure.Messaging;

public class KafkaOptions
{
    public required string BootstrapServers { get; init; }
    public ProducerConfig ProducerConfig { get; init; } = new();

    public ProducerConfig CreateProducerConfig()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers);
        ArgumentNullException.ThrowIfNull(ProducerConfig);
        return new ProducerConfig(ProducerConfig.ToDictionary(entry => entry.Key, entry => entry.Value))
        {
            BootstrapServers = BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageMaxBytes = 1_000_000,
            MessageTimeoutMs = 10_000
        };
    }
}