using System.Text.Json;
using Confluent.Kafka;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Tests.Support;
using WorkflowScheduler.Worker;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class WorkflowWorkerTests
{
    [Fact]
    public async Task TypedHandler_UsesFreshScopeAndDisposesItBeforeCallback()
    {
        var command = Command();
        var second = command with { StepInstanceId = Guid.NewGuid() };
        var client = Client(command);
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true), Task.FromResult(true));
        var trace = new ScopeTrace();
        var services = Services(client);
        services.AddSingleton(trace);
        services.AddScoped<ScopedDependency>();
        services.AddWorkflowSchedulerWorker(Configure).Handle<Input, Output, ScopedHandler>("Charge");
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var processor = provider.GetRequiredService<WorkflowMessageProcessor>();

        await processor.ProcessAsync(command, CancellationToken.None);
        await processor.ProcessAsync(second, CancellationToken.None);

        Assert.Equal(2, trace.Created.Count);
        Assert.Equal(2, trace.Created.Distinct().Count());
        Assert.Equal(trace.Created, trace.Disposed);
        Assert.Equal(new[] { 42, 42 }, trace.Inputs);
        Assert.All(trace.Contexts, context => Assert.Equal("Charge", context.Command));
        var calls = client.ReceivedCalls().Where(call => call.GetMethodInfo().Name == "CompleteStepAsync").ToList();
        Assert.Equal(2, calls.Count);
        var firstOutput = (JsonElement)calls[0].GetArguments()[1]!;
        Assert.Equal(42, firstOutput.GetProperty("orderId").GetInt32());
        Assert.Equal("worker:" + command.StepInstanceId.ToString("N") + ":0", calls[0].GetArguments()[2]);
    }

    [Fact]
    public async Task CallbackRetries_ReuseResultAndIdentityWithoutRepeatingHandler()
    {
        var command = Command() with { Attempt = 2 };
        var client = Client(command);
        var unavailable = await RpcFailure(StatusCode.Unavailable);
        var effects = 0;
        client
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(unavailable), _ => Task.CompletedTask);
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, Output>(
                "Charge",
                (input, context, _) =>
                {
                    effects++;
                    Assert.Equal(command.CorrelationId, context.CorrelationId);
                    Assert.Equal(command.StepInstanceId.ToString("N") + ":2", context.IdempotencyKey);
                    return Task.FromResult(new Output(input.OrderId));
                }));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        Assert.Equal(1, effects);
        var calls = client.ReceivedCalls().Where(call => call.GetMethodInfo().Name == "CompleteStepAsync").ToList();
        Assert.Equal(2, calls.Count);
        Assert.Equal(
            ((JsonElement)calls[0].GetArguments()[1]!).GetRawText(),
            ((JsonElement)calls[1].GetArguments()[1]!).GetRawText());
        Assert.Equal(calls[0].GetArguments()[2], calls[1].GetArguments()[2]);
        Assert.EndsWith(":2", (string)calls[0].GetArguments()[2]!);
    }

    [Fact]
    public async Task LostCallbackResponse_ReconcilesConflictWithPersistedState()
    {
        var command = Command();
        var client = Client(command);
        var unavailable = await RpcFailure(StatusCode.Unavailable);
        var conflict = await RpcFailure(StatusCode.FailedPrecondition);
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(
                Task.FromResult(true),
                Task.FromResult(false));
        client
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(unavailable), _ => Task.FromException(conflict));
        var effects = 0;
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(++effects)));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        Assert.Equal(1, effects);
        await client
            .Received(2)
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>());
        await client
            .Received(2)
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, 3)]
    [InlineData(StatusCode.FailedPrecondition, 3)]
    [InlineData(StatusCode.PermissionDenied, 1)]
    public async Task UnacknowledgedCallback_PropagatesFailureAfterBoundedRetries(StatusCode code, int expectedCalls)
    {
        var command = Command();
        var client = Client(command);
        var failure = await RpcFailure(code);
        client
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromException(failure));
        var effects = 0;
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(++effects)));

        var error = await Assert.ThrowsAsync<WorkflowClientException>(() =>
            provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None));

        Assert.Same(failure, error);
        Assert.Equal(1, effects);
        await client
            .Received(expectedCalls)
            .CompleteStepAsync(
                Arg.Any<StepExecution>(),
                Arg.Any<JsonElement>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TransientStatusFailure_RetriesBeforeRunningHandler()
    {
        var command = Command();
        var client = Client(command);
        var unavailable = await RpcFailure(StatusCode.Unavailable);
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromException<bool>(unavailable),
                _ => Task.FromResult(true));
        var effects = 0;
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(++effects)));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        Assert.Equal(1, effects);
        await client
            .Received(2)
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InactiveCommand_DoesNotInvokeHandlerOrCallback()
    {
        var command = Command();
        var client = Client(command);
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(false));
        var effects = 0;
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(++effects)));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        Assert.Equal(0, effects);
        Assert.DoesNotContain(
            client.ReceivedCalls(),
            call => call.GetMethodInfo().Name is "CompleteStepAsync" or "FailStepAsync");
    }

    [Fact]
    public async Task CompensationCommand_RunsWhileProcessIsCancelled()
    {
        var command = Command() with { IsCompensation = true };
        var client = Client(command);
        WorkflowCommandContext? seen = null;
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, bool>(
                "Charge",
                (_, context, _) =>
                {
                    seen = context;
                    return Task.FromResult(true);
                }));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        Assert.True(seen!.IsCompensation);
        Assert.Equal(command.StepInstanceId, seen.Execution.StepInstanceId);
    }

    [Theory]
    [InlineData("handler")]
    [InlineData("input")]
    [InlineData("unknown")]
    public async Task BusinessFailureInvalidInputAndMissingHandler_ReportFailure(string cause)
    {
        var command = Command();
        switch (cause)
        {
            case "input":
                command = command with { InputData = "{bad json" };
                break;
            case "unknown":
                command = command with { Command = "NotRegistered" };
                break;
        }

        var client = Client(command);
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>(
                "Charge",
                (_, _, _) =>
                    Task.FromException<int>(new InvalidOperationException(new string('x', 3000)))));

        await provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, CancellationToken.None);

        var failure = Assert.Single(client.ReceivedCalls(), call => call.GetMethodInfo().Name == "FailStepAsync");
        var message = (string)failure.GetArguments()[1]!;
        Assert.InRange(message.Length, 1, 2048);
        switch (cause)
        {
            case "handler":
                Assert.Equal(2048, message.Length);
                break;
            case "unknown":
                Assert.Contains("NotRegistered", message);
                break;
        }

        Assert.DoesNotContain(client.ReceivedCalls(), call => call.GetMethodInfo().Name == "CompleteStepAsync");
    }

    [Fact]
    public async Task StoppingDuringHandler_CancelsWithoutCallback()
    {
        var command = Command();
        var client = Client(command);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>(
                "Charge",
                async (_, _, ct) =>
                {
                    entered.SetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return 1;
                }));
        using var cancellation = new CancellationTokenSource();
        var processing =
            provider.GetRequiredService<WorkflowMessageProcessor>().ProcessAsync(command, cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellation.Token);

        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => processing);

        Assert.DoesNotContain(
            client.ReceivedCalls(),
            call => call.GetMethodInfo().Name is "CompleteStepAsync" or "FailStepAsync");
    }

    [Fact]
    public async Task HandlerTimeout_ReportsFailureAndPreCancelledCommandDoesNothing()
    {
        var command = Command();
        var client = Client(command);
        await using var provider = Provider(
            client,
            builder => builder.Handle<Input, int>(
                "Charge",
                async (_, _, ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return 0;
                }),
            options => options.HandlerTimeout = TimeSpan.FromMilliseconds(30));
        var processor = provider.GetRequiredService<WorkflowMessageProcessor>();

        await processor.ProcessAsync(command, CancellationToken.None);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            processor.ProcessAsync(command, new CancellationToken(true)));

        await client
            .Received(1)
            .FailStepAsync(
                Arg.Any<StepExecution>(),
                "Command handler timed out.",
                Arg.Any<string>(),
                Arg.Any<CancellationToken>());
        await client
            .Received(1)
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("{bad")]
    [InlineData("{}")]
    public void MalformedEnvelope_IsRejected(string? payload)
    {
        Assert.Throws<JsonException>(() => WorkflowMessageProcessor.ReadCommand(payload, Guid.NewGuid().ToString()));
    }

    [Fact]
    public void Envelope_ValidatesKeyIdentityAndAttemptAndAcceptsCaseInsensitiveProperties()
    {
        var command = Command();
        var payload = JsonSerializer.Serialize(command, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        Assert.Equal(command, WorkflowMessageProcessor.ReadCommand(payload, command.ProcessInstanceId.ToString()));
        Assert.Throws<JsonException>(() => WorkflowMessageProcessor.ReadCommand(payload, Guid.NewGuid().ToString()));
        Assert.Throws<JsonException>(() => WorkflowMessageProcessor.ReadCommand(
            JsonSerializer.Serialize(command with { Attempt = -1 }),
            command.ProcessInstanceId.ToString()));
        var a = new WorkflowCommandContext(
            new StepExecution(command.ProcessInstanceId, command.StepDefinitionId, command.StepInstanceId, 0),
            "Charge",
            "correlation",
            false);
        Assert.NotEqual(
            a.CallbackCorrelationId,
            (a with { Execution = a.Execution with { Attempt = 1 } }).CallbackCorrelationId);
    }

    [Fact]
    public void Registration_ValidatesHandlersAndCopiesSafeConfiguration()
    {
        var services = Services(Client(Command()));
        Assert.Throws<ArgumentException>(() => services.AddWorkflowSchedulerWorker(_ => { }));
        WorkflowWorkerOptions? original = null;
        var builder = services.AddWorkflowSchedulerWorker(options =>
        {
            Configure(options);
            original = options;
            options.RpcTimeout = TimeSpan.FromSeconds(2);
            options.ConsumerConfig = new ConsumerConfig { EnableAutoOffsetStore = true, EnableAutoCommit = false };
            options.ConsumerConfig.EnableAutoOffsetStore = true;
            options.ConsumerConfig.EnableAutoCommit = false;
        });
        builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(1));
        Assert.Throws<ArgumentException>(() => builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(1)));
        Assert.Throws<InvalidOperationException>(() => services.AddWorkflowSchedulerWorker(Configure));
        original!.ServiceName = "mutated";
        original.JsonSerializerOptions.PropertyNamingPolicy = null;
        using var provider = services.BuildServiceProvider();
        var snapshot = provider.GetRequiredService<WorkerRegistration>().Options;
        Assert.Equal("process.commands.billing", snapshot.Topic);
        Assert.Equal("workflow-billing", snapshot.GroupId);
        Assert.Equal(TimeSpan.FromSeconds(2), snapshot.RpcTimeout);
        Assert.True(snapshot.ConsumerConfig.EnableAutoCommit);
        Assert.False(snapshot.ConsumerConfig.EnableAutoOffsetStore);
        Assert.True(snapshot.JsonSerializerOptions.IsReadOnly);
        Assert.NotNull(snapshot.JsonSerializerOptions.PropertyNamingPolicy);
    }

    [Theory]
    [InlineData("service")]
    [InlineData("topic")]
    [InlineData("same-topic")]
    [InlineData("group")]
    [InlineData("attempts")]
    [InlineData("poll")]
    public void InvalidConfiguration_FailsBeforeStartingKafka(string error)
    {
        var services = new ServiceCollection();
        Assert.ThrowsAny<ArgumentException>(() => services.AddWorkflowSchedulerWorker(options =>
        {
            Configure(options);
            switch (error)
            {
                case "service": options.ServiceName = "billing/orders"; break;
                case "topic": options.DeadLetterTopic = new string('x', 250); break;
                case "same-topic": options.DeadLetterTopic = "process.commands.billing"; break;
                case "group": options.GroupId = " "; break;
                case "attempts": options.MaxDeliveryAttempts = 0; break;
                default: options.ConsumerConfig.MaxPollIntervalMs = 1000; break;
            }
        }));
    }

    [Fact]
    public void EmptyHandlerRegistry_FailsAtStartup()
    {
        var services = Services(Client(Command()));
        services.AddWorkflowSchedulerWorker(Configure);
        using var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetServices<IHostedService>().ToArray());
    }

    private static ServiceCollection Services(IWorkflowSchedulerClient client)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(client);
        return services;
    }

    private static ServiceProvider Provider(IWorkflowSchedulerClient client,
        Action<WorkflowWorkerBuilder> handlers,
        Action<WorkflowWorkerOptions>? configure = null)
    {
        var services = Services(client);
        var builder = services.AddWorkflowSchedulerWorker(options =>
        {
            Configure(options);
            configure?.Invoke(options);
        });
        handlers(builder);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static void Configure(WorkflowWorkerOptions options)
    {
        options.ServiceName = "billing";
        options.BootstrapServers = "localhost:9092";
        options.RetryDelay = TimeSpan.Zero;
        options.MaxDeliveryAttempts = 3;
    }

    private static StepCommandEvent Command()
    {
        return new StepCommandEvent(
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Charge",
            "{\"orderId\":42}",
            "envelope-correlation");
    }

    private static IWorkflowSchedulerClient Client(StepCommandEvent command)
    {
        var client = Substitute.For<IWorkflowSchedulerClient>();
        client
            .IsStepActiveAsync(Arg.Any<StepExecution>(), command.IsCompensation, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        return client;
    }

    private static Task<WorkflowClientException> RpcFailure(StatusCode code)
    {
        return WorkerTestHost.ClientFailureAsync(code);
    }

    public sealed record Input(int OrderId);

    public sealed record Output(int OrderId);

    public sealed class ScopeTrace
    {
        public List<Guid> Created { get; } = [];
        public List<Guid> Disposed { get; } = [];
        public List<int> Inputs { get; } = [];
        public List<WorkflowCommandContext> Contexts { get; } = [];
    }

    public sealed class ScopedDependency(ScopeTrace trace) : IAsyncDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();

        public ValueTask DisposeAsync()
        {
            trace.Disposed.Add(Id);
            return ValueTask.CompletedTask;
        }
    }

    public sealed class ScopedHandler(ScopedDependency dependency, ScopeTrace trace)
        : IWorkflowCommandHandler<Input, Output>
    {
        public Task<Output> HandleAsync(Input input,
            WorkflowCommandContext context,
            CancellationToken cancellationToken)
        {
            trace.Created.Add(dependency.Id);
            trace.Inputs.Add(input.OrderId);
            trace.Contexts.Add(context);
            return Task.FromResult(new Output(input.OrderId));
        }
    }
}