using Confluent.Kafka;

namespace WorkflowScheduler.Common.Abstractions;

public interface IKafkaConsumerFactory
{
    IConsumer<string, string> Create();
}