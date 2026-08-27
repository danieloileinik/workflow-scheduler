namespace WorkflowScheduler.Infrastructure.Messaging;

public class KafkaOptions
{
    public required string BootstrapServers { get; init; }
}