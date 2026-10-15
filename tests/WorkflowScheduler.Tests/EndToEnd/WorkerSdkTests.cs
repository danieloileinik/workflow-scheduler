using System.Collections.Concurrent;
using System.Text.Json;
using Grpc.Core;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Tests.Support;
using WorkflowScheduler.Worker;

namespace WorkflowScheduler.Tests.EndToEnd;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "EndToEnd")]
public class WorkerSdkTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task ClientAndWorker_RunTypedWorkflowAndIgnoreDuplicateKafkaDelivery()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        var handled = new ConcurrentQueue<(Input Input, WorkflowCommandContext Context)>();

        Task<Output> Handle(Input input, WorkflowCommandContext context, CancellationToken _)
        {
            handled.Enqueue((input, context));
            return Task.FromResult(new Output(input.OrderId, context.Command));
        }

        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder =>
                builder.Handle<Input, Output>("Charge", Handle).Handle<Input, Output>("Ship", Handle));
        var definition = await client.RegisterAsync(
            Workflow
                .Define("worker-order-" + Guid.NewGuid())
                .Call(worker.ServiceName, "Charge")
                .WaitFor("Approved")
                .Call(worker.ServiceName, "Ship")
                .Build());

        var process = await client.StartAsync(definition, new Input(42), "order-42", Guid.NewGuid());
        var waiting = await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.StepHistory.Count == 2 && state.StepHistory[1].Status == WorkflowStepStatus.Waiting,
            "Worker finished Charge and process is waiting for Approved");
        var first = Assert.Single(handled).Context;
        await worker.PublishAsync(
            new StepCommandEvent(
                process,
                first.Execution.StepDefinitionId,
                first.Execution.StepInstanceId,
                "Charge",
                JsonSerializer.Serialize(new Input(42)),
                first.CorrelationId));
        await worker.WaitForOffsetAsync(2);
        Assert.Single(handled);
        await client.SendEventAsync(process, "Approved", new { Approved = true }, "approve-42");
        var completed = await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.Status == WorkflowProcessStatus.Completed,
            "Worker finished the workflow");
        await worker.WaitForOffsetAsync(3);

        Assert.Equal(42, waiting.StepHistory[0].ReadOutput<Output>()!.OrderId);
        Assert.Equal(["Charge", "Ship"], handled.Select(item => item.Context.Command));
        Assert.All(
            handled,
            item =>
            {
                Assert.Equal(42, item.Input.OrderId);
                Assert.False(item.Context.IsCompensation);
            });
        Assert.Equal(3, completed.StepHistory.Count);
        Assert.All(completed.StepHistory, step => Assert.Equal(WorkflowStepStatus.Completed, step.Status));
        Assert.Equal("Ship", completed.StepHistory[2].ReadOutput<Output>()!.Command);
        Assert.Equal(2, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "CompleteStep")));
    }

    [Fact]
    public async Task HandlerFailure_UsesSchedulerRetryWithNewAttemptAndCallbackIdentity()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        var attempts = new ConcurrentQueue<WorkflowCommandContext>();
        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder => builder.Handle<Input, Output>(
                "Charge",
                (input, context, _) =>
                {
                    attempts.Enqueue(context);
                    if (context.Execution.Attempt == 0)
                        throw new InvalidOperationException("Temporary payment failure");
                    return Task.FromResult(new Output(input.OrderId, context.Command));
                }));
        var definition = await client.RegisterAsync(
            Workflow.Define("worker-retry-" + Guid.NewGuid()).Call(worker.ServiceName, "Charge").Build());

        var process = await client.StartAsync(definition, new Input(42), "order", Guid.NewGuid());
        var pending = await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.StepHistory.Count == 1 && state.StepHistory[0].Status == WorkflowStepStatus.Pending,
            "Worker failure scheduled a retry");
        await app.WithDb(db => db
            .StepInstances
            .Where(step => step.Id == pending.StepHistory[0].Id)
            .ExecuteUpdateAsync(update => update.SetProperty(
                step => step.NextRetryAt,
                DateTime.UtcNow.AddSeconds(-1))));
        var completed = await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.Status == WorkflowProcessStatus.Completed,
            "Worker completed the retried attempt");
        await worker.WaitForOffsetAsync(2);

        var observed = attempts.ToArray();
        Assert.Equal([0, 1], observed.Select(context => context.Execution.Attempt));
        Assert.Equal(observed[0].Execution.StepInstanceId, observed[1].Execution.StepInstanceId);
        Assert.NotEqual(observed[0].CallbackCorrelationId, observed[1].CallbackCorrelationId);
        Assert.NotEqual(observed[0].IdempotencyKey, observed[1].IdempotencyKey);
        Assert.Equal(1, completed.StepHistory.Single().RetryCount);
        Assert.Equal(42, completed.StepHistory.Single().ReadOutput<Output>()!.OrderId);
        Assert.Equal(2, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "CompleteStep")));
    }

    [Fact]
    public async Task Cancellation_ExecutesRegisteredCompensationHandler()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        var executions = new ConcurrentQueue<WorkflowCommandContext>();

        Task<Output> Handle(Input input, WorkflowCommandContext context, CancellationToken _)
        {
            executions.Enqueue(context);
            return Task.FromResult(new Output(input.OrderId, context.Command));
        }

        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            client,
            builder =>
                builder.Handle<Input, Output>("Charge", Handle).Handle<Input, Output>("Refund", Handle));
        var definition = await client.RegisterAsync(
            Workflow
                .Define("worker-compensation-" + Guid.NewGuid())
                .Call(worker.ServiceName, "Charge")
                .CompensateWith(worker.ServiceName, "Refund")
                .WaitFor("Approved")
                .Build());

        var process = await client.StartAsync(definition, new Input(42), "order", Guid.NewGuid());
        await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.StepHistory.Count == 2 && state.StepHistory[1].Status == WorkflowStepStatus.Waiting,
            "Forward call is complete");
        await client.CancelAsync(process);
        var cancelled = await Eventually.UntilAsync(
            ct => client.GetStatusAsync(process, ct),
            state => state.StepHistory.Count == 3 && state.StepHistory[0].Status == WorkflowStepStatus.Compensated,
            "Worker completed compensation");
        await worker.WaitForOffsetAsync(2);

        Assert.Equal(WorkflowProcessStatus.Cancelled, cancelled.Status);
        Assert.Equal(WorkflowStepStatus.Completed, cancelled.StepHistory[2].Status);
        var observed = executions.ToArray();
        Assert.Equal(["Charge", "Refund"], observed.Select(context => context.Command));
        Assert.False(observed[0].IsCompensation);
        Assert.True(observed[1].IsCompensation);
    }

    [Fact]
    public async Task LostGrpcAcknowledgement_RetriesCallbackWithoutRepeatingBusinessOperation()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        var transport = new AckLosingClient(client, await WorkerTestHost.ClientFailureAsync(StatusCode.Unavailable));
        var effects = 0;
        await using var worker = await WorkerTestHost.StartAsync(
            infrastructure,
            transport,
            builder =>
                builder.Handle<Input, int>("Charge", (_, _, _) => Task.FromResult(Interlocked.Increment(ref effects))));
        var definition = await client.RegisterAsync(
            Workflow.Define("worker-ack-" + Guid.NewGuid()).Call(worker.ServiceName, "Charge").Build());

        var process = await client.StartAsync(definition, new Input(42), "order", Guid.NewGuid());
        await worker.WaitForOffsetAsync(1);
        var completed = await client.GetStatusAsync(process);

        Assert.Equal(WorkflowProcessStatus.Completed, completed.Status);
        Assert.Equal(1, effects);
        Assert.Equal(2, transport.CompleteAttempts);
        Assert.Equal(1, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "CompleteStep")));
    }

    public sealed record Input(int OrderId);

    [Fact]
    public async Task LargeAccumulatedOutputs_DoNotPreventFollowingCommandsFromRunning()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client = new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        var output = new string('a', 1_100_000);
        var effects = 0;
        await using var worker = await WorkerTestHost.StartAsync(infrastructure, client,
            builder => builder.Handle<Input, string>("Large", (_, _, _) =>
            {
                Interlocked.Increment(ref effects);
                return Task.FromResult(output);
            }));
        var workflow = Workflow.Define("large-history-" + Guid.NewGuid());
        for (var index = 0; index < 5; index++) workflow.Call(worker.ServiceName, "Large");
        var definition = await client.RegisterAsync(workflow.Build());

        var process = await client.StartAsync(definition, new Input(42), "order", Guid.NewGuid());
        await worker.WaitForOffsetAsync(5);

        Assert.Equal(5, effects);
        Assert.Equal(ProcessStatus.Completed, await app.WithDb(db => db.ProcessInstances
            .Where(instance => instance.Id == process).Select(instance => instance.Status).SingleAsync()));
        var tooLarge = await Assert.ThrowsAsync<WorkflowClientException>(() => client.GetStatusAsync(process));
        Assert.Equal(StatusCode.ResourceExhausted, tooLarge.StatusCode);
    }

    public sealed record Output(int OrderId, string Command);

    private sealed class AckLosingClient(IWorkflowSchedulerClient inner, WorkflowClientException failure)
        : IWorkflowSchedulerClient
    {
        public int CompleteAttempts;

        public async Task CompleteStepAsync<TOutput>(StepExecution execution,
            TOutput output,
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            var attempt = Interlocked.Increment(ref CompleteAttempts);
            await inner.CompleteStepAsync(execution, output, correlationId, cancellationToken);
            if (attempt == 1) throw failure;
        }

        public Task<RegisteredWorkflow> RegisterAsync(WorkflowDefinition definition,
            CancellationToken cancellationToken = default)
        {
            return inner.RegisterAsync(definition, cancellationToken);
        }

        public Task<Guid> StartAsync<TInput>(Guid definitionId,
            TInput input,
            string businessKey,
            Guid idempotencyKey,
            CancellationToken cancellationToken = default)
        {
            return inner.StartAsync(definitionId, input, businessKey, idempotencyKey, cancellationToken);
        }

        public Task<Guid> StartAsync<TInput>(RegisteredWorkflow definition,
            TInput input,
            string businessKey,
            Guid idempotencyKey,
            CancellationToken cancellationToken = default)
        {
            return inner.StartAsync(definition, input, businessKey, idempotencyKey, cancellationToken);
        }

        public Task SendEventAsync<TPayload>(Guid processInstanceId,
            string eventName,
            TPayload payload,
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            return inner.SendEventAsync(processInstanceId, eventName, payload, correlationId, cancellationToken);
        }

        public Task FailStepAsync(StepExecution execution,
            string error,
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            return inner.FailStepAsync(execution, error, correlationId, cancellationToken);
        }

        public Task CancelAsync(Guid processInstanceId, CancellationToken cancellationToken = default)
        {
            return inner.CancelAsync(processInstanceId, cancellationToken);
        }

        public Task<WorkflowProcessSnapshot> GetStatusAsync(Guid processInstanceId,
            CancellationToken cancellationToken = default)
        {
            return inner.GetStatusAsync(processInstanceId, cancellationToken);
        }

        public Task<bool> IsStepActiveAsync(StepExecution execution, bool isCompensation = false,
            CancellationToken cancellationToken = default) =>
            inner.IsStepActiveAsync(execution, isCompensation, cancellationToken);

        public Task<RegisteredWorkflow> GetDefinitionAsync(string name, int version,
            CancellationToken cancellationToken = default) => inner.GetDefinitionAsync(name, version, cancellationToken);
    }
}
