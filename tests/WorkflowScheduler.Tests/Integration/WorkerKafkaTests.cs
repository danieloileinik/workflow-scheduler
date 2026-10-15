using System.Text.Json;
using Confluent.Kafka;
using Grpc.Core;
using NSubstitute;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Tests.Support;
using WorkflowScheduler.Worker;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class WorkerKafkaTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task OffsetIsStoredAfterCallbackAndDuplicateDeliveryDoesNotRepeatBusinessOperation()
    {
        var command = Command();
        var client = Client(command);
        var acknowledged = 0;
        var effects = 0;
        var callback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(_ =>
                Task.FromResult(
                    Volatile.Read(ref acknowledged) == 0));
        client
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                callback.TrySetResult();
                await release.Task.WaitAsync(call.ArgAt<CancellationToken>(3));
                Interlocked.Exchange(ref acknowledged, 1);
            });
        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder =>
                builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(Interlocked.Increment(ref effects))));
        try
        {
            await worker.PublishAsync(command);
            await callback.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(Offset.Unset.Value, await worker.CommittedAsync(CancellationToken.None));
        }
        finally
        {
            release.TrySetResult();
        }

        await worker.WaitForOffsetAsync(1);
        await worker.PublishAsync(command);
        await worker.WaitForOffsetAsync(2);

        Assert.Equal(1, effects);
        await client
            .Received(1)
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExhaustedCallbackRetries_LeaveKafkaRecordUncommitted()
    {
        var command = Command();
        var client = Client(command);
        var unavailable = await WorkerTestHost.ClientFailureAsync(StatusCode.Unavailable);
        client
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromException(unavailable));
        var effects = 0;
        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder =>
                builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(Interlocked.Increment(ref effects))));

        await worker.PublishAsync(command);
        await Assert.ThrowsAsync<WorkflowClientException>(() => worker.Execution.WaitAsync(TimeSpan.FromSeconds(20)));

        Assert.Equal(1, effects);
        Assert.Equal(Offset.Unset.Value, await worker.CommittedAsync(CancellationToken.None));
        await client
            .Received(3)
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MalformedRecord_IsCommittedOnlyAfterDeadLetterWasPublished(bool oversizedEnvelope)
    {
        var client = Substitute.For<IWorkflowSchedulerClient>();
        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder =>
                builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(0)));
        using var consumer = new ConsumerBuilder<string, string>(
            new ConsumerConfig
            {
                BootstrapServers = infrastructure.BootstrapServers, GroupId = "dead-letter-probe-" + Guid.NewGuid(),
                EnableAutoCommit = false
            }).Build();
        consumer.Assign(new TopicPartitionOffset(worker.Topic + ".dead-letter", new Partition(0), Offset.Beginning));
        var invalid = oversizedEnvelope ? new string('"', 700_000) : "{not-json";

        await worker.PublishRawAsync("bad-key", invalid);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var record = await Task.Run(() => consumer.Consume(timeout.Token), timeout.Token);
        await worker.WaitForOffsetAsync(1);

        var letter = JsonSerializer.Deserialize<WorkflowDeadLetter>(record.Message.Value)!;
        Assert.Equal(worker.Topic, letter.Topic);
        Assert.Equal(0, letter.Partition);
        Assert.Equal(0, letter.Offset);
        Assert.Equal("bad-key", letter.Key);
        if (oversizedEnvelope)
        {
            Assert.True(letter.PayloadTruncated);
            Assert.Equal(700_000, letter.OriginalPayloadBytes);
            Assert.NotEmpty(letter.PayloadSha256!);
        }
        else Assert.Equal(invalid, letter.Payload);
        Assert.Equal(worker.Topic + ":0:0", record.Message.Key);
        Assert.NotEmpty(letter.Error);
        Assert.Empty(client.ReceivedCalls());
    }

    [Fact]
    public async Task StopDuringHandler_RedeliversUncommittedRecordToNextWorker()
    {
        var command = Command();
        var client = Client(command);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var first = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder => builder.Handle<Input, int>(
                "Charge",
                async (_, _, ct) =>
                {
                    entered.SetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return 0;
                }));
        await first.PublishAsync(command);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(20));

        await first.StopAsync();
        Assert.Equal(Offset.Unset.Value, await first.CommittedAsync(CancellationToken.None));
        Assert.DoesNotContain(
            client.ReceivedCalls(),
            call => call.GetMethodInfo().Name is "CompleteStepAsync" or "FailStepAsync");
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var second = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder => builder.Handle<Input, int>(
                "Charge",
                (_, _, _) =>
                {
                    completed.SetResult();
                    return Task.FromResult(42);
                }),
            first.ServiceName,
            first.GroupId,
            false);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await second.WaitForOffsetAsync(1);

        await client
            .Received(1)
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
    }

    private static StepCommandEvent Command()
    {
        return new StepCommandEvent(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Charge",
            "{\"orderId\":42}",
            "broker-correlation");
    }

    private static IWorkflowSchedulerClient Client(StepCommandEvent command)
    {
        var client = Substitute.For<IWorkflowSchedulerClient>();
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        return client;
    }

    private sealed record Input(int OrderId);
}