using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WorkflowScheduler.Worker;

internal sealed class WorkflowWorker(
    WorkerRegistration registration,
    WorkflowMessageProcessor processor,
    ILogger<WorkflowWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = registration.Options;
        var retry = new WorkerRetry(options, logger);
        using var consumer = new ConsumerBuilder<string, string>(options.ConsumerConfig)
            .SetPartitionsRevokedHandler((_, _) => { })
            .SetErrorHandler((_, error) => logger.LogWarning("Kafka consumer: {Reason}", error.Reason))
            .Build();
        using var deadLetters = new ProducerBuilder<string, string>(options.DeadLetterProducerConfig).Build();
        consumer.Subscribe(options.Topic);
        logger.LogInformation("Workflow worker consuming {Topic} in group {GroupId}", options.Topic, options.GroupId);
        var consumeFailures = 0;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> record;
                try
                {
                    record = consumer.Consume(stoppingToken);
                    consumeFailures = 0;
                }
                catch (KafkaException exception) when (!exception.Error.IsFatal
                    && ++consumeFailures < options.MaxDeliveryAttempts)
                {
                    logger.LogWarning(exception, "Kafka consume failed; retry {Attempt}", consumeFailures);
                    await Task.Delay(options.RetryDelay, stoppingToken);
                    continue;
                }
                if (record.IsPartitionEOF) continue;
                WorkflowScheduler.Contracts.Events.StepCommandEvent? command = null;
                string? malformed = null;
                try
                {
                    command = WorkflowMessageProcessor.ReadCommand(record.Message?.Value, record.Message?.Key);
                }
                catch (JsonException exception)
                {
                    malformed = exception.Message;
                }
                if (command is not null)
                {
                    await processor.ProcessAsync(command, stoppingToken);
                }
                else
                {
                    var letter = new WorkflowDeadLetter(record.Topic, record.Partition.Value, record.Offset.Value,
                        record.Message?.Key, record.Message?.Value, malformed!);
                    var message = new Message<string, string>
                    {
                        Key = $"{record.Topic}:{record.Partition.Value}:{record.Offset.Value}",
                        Value = JsonSerializer.Serialize(letter)
                    };
                    await retry.RunAsync("Kafka dead-letter", async ct =>
                        await deadLetters.ProduceAsync(options.DeadLetterTopic!, message, ct), stoppingToken);
                    logger.LogWarning("Malformed command sent to {DeadLetterTopic} from {Position}",
                        options.DeadLetterTopic, record.TopicPartitionOffset);
                }
                stoppingToken.ThrowIfCancellationRequested();
                try
                {
                    consumer.StoreOffset(record);
                }
                catch (KafkaException exception) when (exception.Error.Code == ErrorCode.Local_State)
                {
                    logger.LogWarning(exception, "Partition ownership lost at {Position}; record may be redelivered",
                        record.TopicPartitionOffset);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // A partially handled record has never been stored or made eligible for commit.
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (KafkaException exception)
            {
                logger.LogWarning(exception, "Kafka consumer could not leave its group cleanly");
            }
        }
    }
}
