using System.Text.Json;
using Confluent.Kafka;
using WorkflowScheduler.Contracts.Grpc;

namespace WorkflowScheduler.Tests.Support;

internal sealed class KafkaProbe : IDisposable
{
    private readonly List<(string Topic, string Key, ExecuteStepCommand Command)> _buffer = [];
    private readonly IConsumer<string, string> _consumer;
    private readonly Dictionary<TopicPartition, Offset> _nextOffsets = new();

    public KafkaProbe(string bootstrapServers, string[] services)
    {
        _consumer = new ConsumerBuilder<string, string>(
            new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "workflow-test-" + Guid.NewGuid(),
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Error,
                FetchWaitMaxMs = 50
            }).Build();
        try
        {
            // Capture end offsets BEFORE triggering a workflow. Old test records are ignored
            // without deleting topics or waiting for a consumer group rebalance.
            _consumer.Assign(
            [
                .. services
                    .Select(service =>
                    {
                        var partition = new TopicPartition($"process.commands.{service}", new Partition(0));
                        var end = _consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High;
                        _nextOffsets.Add(partition, end);
                        return new TopicPartitionOffset(partition, end);
                    })
            ]);
        }
        catch
        {
            _consumer.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }

    public async Task<ExecuteStepCommand> ReadAsync(string service, Guid processId)
    {
        var topic = $"process.commands.{service}";
        var buffered =
            _buffer.FindIndex(record => record.Topic == topic && record.Command.ProcessInstanceId == processId);
        if (buffered >= 0)
        {
            var command = _buffer[buffered].Command;
            _buffer.RemoveAt(buffered);
            return command;
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        try
        {
            return await Task.Run(
                () =>
                {
                    while (true)
                    {
                        var record = _consumer.Consume(timeout.Token);
                        _nextOffsets[record.TopicPartition] = record.Offset + 1;
                        var command = JsonSerializer.Deserialize<ExecuteStepCommand>(record.Message.Value)!;
                        Assert.Equal(command.ProcessInstanceId.ToString(), record.Message.Key);
                        if (record.Topic == topic && command.ProcessInstanceId == processId) return command;
                        _buffer.Add((record.Topic, record.Message.Key, command));
                    }
                },
                timeout.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            Assert.Fail($"Kafka did not deliver {topic} for process {processId} within 20 seconds.");
            throw;
        }
    }

    // Call after the outbox is committed. Comparing offsets detects extra delivery
    // without a sleep to see whether another record arrives.
    public int CountRemaining(Guid processId)
    {
        var count = _buffer.Count(record => record.Command.ProcessInstanceId == processId);
        var ends = _consumer.Assignment.ToDictionary(
            partition => partition,
            partition => _consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (ends.Any(partition => _nextOffsets[partition.Key] < partition.Value))
        {
            var record = _consumer.Consume(timeout.Token);
            _nextOffsets[record.TopicPartition] = record.Offset + 1;
            var command = JsonSerializer.Deserialize<ExecuteStepCommand>(record.Message.Value)!;
            if (command.ProcessInstanceId == processId) count++;
            else _buffer.Add((record.Topic, record.Message.Key, command));
        }

        return count;
    }
}