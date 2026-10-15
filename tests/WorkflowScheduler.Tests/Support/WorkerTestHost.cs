using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Grpc;
using WorkflowScheduler.Worker;
using GrpcStatusResponse = WorkflowScheduler.Grpc.GetProcessStatusResponse;
using Metadata = Grpc.Core.Metadata;

namespace WorkflowScheduler.Tests.Support;

internal sealed class WorkerTestHost : IAsyncDisposable
{
    private readonly IConsumer<string, string> _offsets;
    private readonly IProducer<string, string> _producer;
    private readonly ServiceProvider _services;
    private readonly IHostedService _worker;

    private WorkerTestHost(string bootstrap,
        string serviceName,
        string groupId,
        IWorkflowSchedulerClient client,
        Action<WorkflowWorkerBuilder> handlers,
        int deliveryAttempts)
    {
        ServiceName = serviceName;
        GroupId = groupId;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(client);
        handlers(
            services.AddWorkflowSchedulerWorker(options =>
            {
                options.ServiceName = serviceName;
                options.BootstrapServers = bootstrap;
                options.GroupId = groupId;
                options.MaxDeliveryAttempts = deliveryAttempts;
                options.RetryDelay = TimeSpan.FromMilliseconds(10);
                options.ConsumerConfig.AutoCommitIntervalMs = 50;
                options.ConsumerConfig.FetchWaitMaxMs = 10;
            }));
        _services = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        _worker = _services.GetServices<IHostedService>().Single();
        _producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = bootstrap, EnableIdempotence = true }).Build();
        _offsets = new ConsumerBuilder<string, string>(
            new ConsumerConfig
                { BootstrapServers = bootstrap, GroupId = groupId, EnableAutoCommit = false }).Build();
    }

    public string ServiceName { get; }
    public string GroupId { get; }
    public string Topic => "process.commands." + ServiceName;
    public Task Execution => ((BackgroundService)_worker).ExecuteTask!;

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopAsync();
        }
        finally
        {
            _producer.Dispose();
            _offsets.Dispose();
            await _services.DisposeAsync();
        }
    }

    public static async Task<WorkerTestHost> StartAsync(TestInfrastructure infrastructure,
        IWorkflowSchedulerClient client,
        Action<WorkflowWorkerBuilder> handlers,
        string? serviceName = null,
        string? groupId = null,
        bool createTopics = true,
        int deliveryAttempts = 3)
    {
        serviceName ??= "worker-" + Guid.NewGuid().ToString("N");
        groupId ??= "worker-test-" + serviceName;
        if (createTopics)
        {
            using var admin =
                new AdminClientBuilder(new AdminClientConfig { BootstrapServers = infrastructure.BootstrapServers })
                    .Build();
            await admin.CreateTopicsAsync(
                new[] { "process.commands." + serviceName, "process.commands." + serviceName + ".dead-letter" }
                    .Select(topic => new TopicSpecification
                        { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }));
        }

        var host = new WorkerTestHost(
            infrastructure.BootstrapServers,
            serviceName,
            groupId,
            client,
            handlers,
            deliveryAttempts);
        try
        {
            await host._worker.StartAsync(CancellationToken.None);
            return host;
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }
    }

    public Task PublishAsync(StepCommandEvent command)
    {
        return PublishRawAsync(
            command.ProcessInstanceId.ToString(),
            JsonSerializer.Serialize(command));
    }

    public async Task PublishRawAsync(string? key, string? payload)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await _producer.ProduceAsync(
            Topic,
            new Message<string, string> { Key = key!, Value = payload! },
            timeout.Token);
    }

    public Task<long> CommittedAsync(CancellationToken _)
    {
        return Task.FromResult(
            _offsets
                .Committed([new TopicPartition(Topic, new Partition(0))], TimeSpan.FromSeconds(5))
                .Single()
                .Offset
                .Value);
    }

    public Task WaitForOffsetAsync(long offset)
    {
        return Eventually.UntilAsync(
            CommittedAsync,
            value => value >= offset,
            $"Worker group {GroupId} committed offset {offset}");
    }

    public async Task StopAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await _worker.StopAsync(timeout.Token);
    }

    internal static async Task<WorkflowClientException> ClientFailureAsync(StatusCode code)
    {
        var invoker = Substitute.For<CallInvoker>();
        invoker
            .AsyncUnaryCall(
                Arg.Any<Method<GetProcessStatusRequest, GrpcStatusResponse>>(),
                Arg.Any<string>(),
                Arg.Any<CallOptions>(),
                Arg.Any<GetProcessStatusRequest>())
            .Returns(_ => new AsyncUnaryCall<GrpcStatusResponse>(
                Task.FromException<GrpcStatusResponse>(
                    new RpcException(new Status(code, "simulated acknowledgement loss"))),
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => [],
                () => { }));
        using var client = new WorkflowSchedulerClient(new Processes.ProcessesClient(invoker));
        return await Assert.ThrowsAsync<WorkflowClientException>(() => client.GetStatusAsync(Guid.NewGuid()));
    }
}