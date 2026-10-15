using System.Text.Json;
using Confluent.Kafka;

namespace WorkflowScheduler.Worker;

public sealed class WorkflowWorkerOptions
{
    public string ServiceName { get; set; } = "";
    public string BootstrapServers { get; set; } = "";
    public string? GroupId { get; set; }
    public string? DeadLetterTopic { get; set; }
    public int MaxDeliveryAttempts { get; set; } = 5;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan RpcTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan HandlerTimeout { get; set; } = TimeSpan.FromMinutes(1);
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);
    public ConsumerConfig ConsumerConfig { get; set; } = new();
    public ProducerConfig DeadLetterProducerConfig { get; set; } = new();

    internal string Topic => "process.commands." + ServiceName;

    internal WorkflowWorkerOptions CopyAndValidate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ServiceName);
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers);
        ArgumentNullException.ThrowIfNull(JsonSerializerOptions);
        ArgumentNullException.ThrowIfNull(ConsumerConfig);
        ArgumentNullException.ThrowIfNull(DeadLetterProducerConfig);
        if (MaxDeliveryAttempts is < 1 or > 20) throw new ArgumentOutOfRangeException(nameof(MaxDeliveryAttempts));
        if (RetryDelay < TimeSpan.Zero || RetryDelay > TimeSpan.FromMinutes(1))
            throw new ArgumentOutOfRangeException(nameof(RetryDelay));
        if (RpcTimeout <= TimeSpan.Zero || RpcTimeout > TimeSpan.FromMinutes(5))
            throw new ArgumentOutOfRangeException(nameof(RpcTimeout));
        if (HandlerTimeout <= TimeSpan.Zero || HandlerTimeout > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(HandlerTimeout));
        var group = GroupId ?? "workflow-" + ServiceName;
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        var deadLetter = DeadLetterTopic ?? Topic + ".dead-letter";
        ValidateTopic(Topic);
        ValidateTopic(deadLetter);
        if (deadLetter == Topic)
            throw new ArgumentException("Dead-letter and command topics must differ.", nameof(DeadLetterTopic));

        var consumer = new ConsumerConfig(ConsumerConfig)
        {
            BootstrapServers = BootstrapServers, GroupId = group,
            // Store only acknowledged results; librdkafka batches commits in the background.
            EnableAutoCommit = true, EnableAutoOffsetStore = false
        };
        consumer.AutoOffsetReset ??= AutoOffsetReset.Earliest;
        consumer.AutoCommitIntervalMs ??= 1000;
        consumer.MaxPollIntervalMs ??= 900_000;
        var budget = HandlerTimeout + (RpcTimeout + RetryDelay) * (MaxDeliveryAttempts * (MaxDeliveryAttempts + 2));
        if (consumer.MaxPollIntervalMs <= budget.TotalMilliseconds)
            throw new ArgumentException(
                "MaxPollIntervalMs must exceed the handler and RPC retry budget.",
                nameof(ConsumerConfig));
        var producer = new ProducerConfig(DeadLetterProducerConfig)
        {
            BootstrapServers = BootstrapServers, EnableIdempotence = true, Acks = Acks.All
        };
        producer.MessageTimeoutMs ??= (int)RpcTimeout.TotalMilliseconds;
        var json = new JsonSerializerOptions(JsonSerializerOptions);
        json.MakeReadOnly(true);
        return new WorkflowWorkerOptions
        {
            ServiceName = ServiceName, BootstrapServers = BootstrapServers, GroupId = group,
            DeadLetterTopic = deadLetter,
            MaxDeliveryAttempts = MaxDeliveryAttempts, RetryDelay = RetryDelay, RpcTimeout = RpcTimeout,
            HandlerTimeout = HandlerTimeout, JsonSerializerOptions = json,
            ConsumerConfig = consumer, DeadLetterProducerConfig = producer
        };
    }

    private static void ValidateTopic(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic) || topic.Length > 249 || topic is "." or ".."
            || topic.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-')))
            throw new ArgumentException(
                "Kafka topics must contain at most 249 ASCII letters, digits, dots, underscores or hyphens.");
    }
}
