using System.Text.Json;
using Grpc.Core;
using WorkflowScheduler.Client;
using WorkflowScheduler.Grpc;
using GrpcStatusResponse = WorkflowScheduler.Grpc.GetProcessStatusResponse;
using RegisterProcessDefinitionResponse = WorkflowScheduler.Grpc.RegisterProcessDefinitionResponse;
using StartProcessResponse = WorkflowScheduler.Grpc.StartProcessResponse;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class WorkflowSchedulerClientTests
{
    [Fact]
    public async Task Start_MapsTypedPayloadAndPreservesIdempotencyIdentityAcrossCalls()
    {
        var definition = Guid.NewGuid();
        var key = Guid.NewGuid();
        var process = Guid.NewGuid();
        var invoker = new RecordingInvoker(_ => new StartProcessResponse
            { ProcessInstanceId = process.ToString() });
        using var client = Client(invoker);
        var first = await client.StartAsync(definition, new { OrderId = 42 }, "order-42", key);
        var replay = await client.StartAsync(definition, new { OrderId = 42 }, "order-42", key);
        Assert.Equal(process, first);
        Assert.Equal(first, replay);
        Assert.All(
            invoker.Requests,
            record =>
            {
                var request = Assert.IsType<StartProcessRequest>(record.Request);
                Assert.Equal(definition.ToString(), request.DefinitionId);
                Assert.Equal(key.ToString(), request.IdempotencyKey);
                Assert.Equal("order-42", request.BusinessKey);
                using var payload = JsonDocument.Parse(request.InputData);
                Assert.Equal(42, payload.RootElement.GetProperty("orderId").GetInt32());
            });
    }

    [Fact]
    public async Task Register_MapsFluentDefinitionWithoutChangingItsIdentifiers()
    {
        var id = Guid.NewGuid();
        var invoker = new RecordingInvoker(_ => new RegisterProcessDefinitionResponse
            { DefinitionId = id.ToString() });
        using var client = Client(invoker);
        var definition = Workflow
            .Define("order", 2)
            .Call("billing", "Charge")
            .CompensateWith("billing", "Refund")
            .WaitFor("Approved")
            .Build();
        var registered = await client.RegisterAsync(definition);
        Assert.Equal(new RegisteredWorkflow(id, "order", 2), registered);
        var request = Assert.IsType<RegisterProcessDefinitionRequest>(Assert.Single(invoker.Requests).Request);
        Assert.Equal(definition.Steps.Select(step => step.Id.ToString()), request.Steps.Select(step => step.Id));
        Assert.Equal(definition.Steps[0].CompensationStepId!.Value.ToString(), request.Steps[0].CompensationStepId);
        Assert.Equal(StepTypeMessage.WaitEvent, request.Steps[1].StepType);
    }

    [Fact]
    public async Task EventsAndCallbacks_PreserveCorrelationAndCurrentAttempt()
    {
        var invoker = new RecordingInvoker(method =>
            method == "SendEvent" ? new SendEventResponse() : new CompleteStepResponse());
        using var client = Client(invoker);
        var execution = new StepExecution(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), 2);
        await client.SendEventAsync(execution.ProcessInstanceId, "Approved", new { Actor = "manager" }, "event-1");
        await client.CompleteStepAsync(execution, new { Reserved = true }, "callback-1");
        await client.FailStepAsync(execution, "temporary error", "callback-2");
        var sent = Assert.IsType<SendEventRequest>(invoker.Requests[0].Request);
        Assert.Equal("event-1", sent.CorrelationId);
        Assert.Equal("{\"actor\":\"manager\"}", sent.Payload);
        var completed = Assert.IsType<CompleteStepRequest>(invoker.Requests[1].Request);
        Assert.Equal(execution.StepInstanceId.ToString(), completed.StepInstanceId);
        Assert.Equal(execution.StepDefinitionId.ToString(), completed.StepDefinitionId);
        Assert.Equal(2, completed.Attempt);
        Assert.True(completed.Success);
        Assert.Equal("callback-1", completed.CorrelationId);
        var failed = Assert.IsType<CompleteStepRequest>(invoker.Requests[2].Request);
        Assert.False(failed.Success);
        Assert.Equal("temporary error", failed.OutputData);
        Assert.Equal("callback-2", failed.CorrelationId);
    }

    [Fact]
    public async Task Status_MapsIdentifiersEnumsTimestampsAndOptionalFields()
    {
        var process = Guid.NewGuid();
        var step = Guid.NewGuid();
        var definitionStep = Guid.NewGuid();
        var at = DateTimeOffset.UtcNow;
        var response = new GrpcStatusResponse
        {
            Id = process.ToString(), DefinitionId = Guid.NewGuid().ToString(), BusinessKey = "order",
            Status = "Waiting", CurrentStepId = step.ToString(), CreatedAt = at.ToString("O"),
            UpdatedAt = at.ToString("O")
        };
        response.StepHistory.Add(
            new StepHistoryEntry
            {
                Id = step.ToString(), StepDefinitionId = definitionStep.ToString(), Status = "Running",
                StartedAt = at.ToString("O"), RetryCount = 2
            });
        using var client = Client(new RecordingInvoker(_ => response));
        var status = await client.GetStatusAsync(process);
        Assert.Equal(WorkflowProcessStatus.Waiting, status.Status);
        Assert.Equal(step, status.CurrentStepId);
        Assert.Equal(at, status.CreatedAt);
        var history = Assert.Single(status.StepHistory);
        Assert.Equal(WorkflowStepStatus.Running, history.Status);
        Assert.Null(history.CompletedAt);
        Assert.Null(history.OutputData);
        Assert.Null(history.Error);
        Assert.Equal(new StepExecution(process, definitionStep, step, 2), history.Execution(process));
    }

    [Fact]
    public async Task Calls_HaveDeadlinesCancellationAndFreshMetadata()
    {
        var invoker = new RecordingInvoker(_ => new CancelProcessResponse());
        var calls = 0;
        using var cancellation = new CancellationTokenSource();
        using var client = Client(
            invoker,
            new WorkflowClientOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(5),
                HeadersFactory = () => new Metadata { { "authorization", $"token-{++calls}" } }
            });
        var before = DateTime.UtcNow;
        await client.CancelAsync(Guid.NewGuid(), cancellation.Token);
        await client.CancelAsync(Guid.NewGuid(), cancellation.Token);
        Assert.Equal(cancellation.Token, invoker.Requests[0].Options.CancellationToken);
        Assert.InRange(
            invoker.Requests[0].Options.Deadline!.Value,
            before.AddSeconds(5),
            DateTime.UtcNow.AddSeconds(5));
        Assert.Equal("token-1", invoker.Requests[0].Options.Headers!.GetValue("authorization"));
        Assert.Equal("token-2", invoker.Requests[1].Options.Headers!.GetValue("authorization"));
    }

    [Theory]
    [InlineData(StatusCode.NotFound)]
    [InlineData(StatusCode.FailedPrecondition)]
    [InlineData(StatusCode.Unavailable)]
    [InlineData(StatusCode.DeadlineExceeded)]
    public async Task RpcFailures_ArePreservedWithoutAutomaticRetries(StatusCode code)
    {
        var rpc = new RpcException(new Status(code, "detail"), new Metadata { { "reason", "test" } });
        var invoker = new RecordingInvoker(_ => throw rpc);
        using var client = Client(invoker);
        var error = await Assert.ThrowsAsync<WorkflowClientException>(() => client.GetStatusAsync(Guid.NewGuid()));
        Assert.Equal(code, error.StatusCode);
        Assert.Equal("detail", error.Detail);
        Assert.Equal("GetStatusAsync", error.Operation);
        Assert.Same(rpc, error.InnerException);
        Assert.Equal("test", error.Trailers.GetValue("reason"));
        Assert.Single(invoker.Requests);
    }

    [Fact]
    public async Task InvalidInputAndPreCancelledCalls_NeverReachTransport()
    {
        var invoker = new RecordingInvoker(_ => new CancelProcessResponse());
        using var client = Client(invoker);
        await Assert.ThrowsAsync<ArgumentException>(() => client.StartAsync(
            Guid.NewGuid(),
            "raw JSON string",
            "order",
            Guid.NewGuid()));
        await Assert.ThrowsAsync<ArgumentException>(() => client.StartAsync(
            Guid.NewGuid(),
            new { },
            "order",
            Guid.Empty));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.CancelAsync(
            Guid.NewGuid(),
            cancelled.Token));
        Assert.Empty(invoker.Requests);
    }

    [Fact]
    public async Task CancellationDuringRpc_RemainsOperationCanceledException()
    {
        using var cancelled = new CancellationTokenSource();
        var invoker = new RecordingInvoker(_ =>
        {
            cancelled.Cancel();
            throw new RpcException(new Status(StatusCode.Cancelled, "cancelled"));
        });
        using var client = Client(invoker);
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.CancelAsync(Guid.NewGuid(), cancelled.Token));
        Assert.Equal(cancelled.Token, error.CancellationToken);
    }

    [Fact]
    public async Task JsonOptions_AreCopiedAndDisposedClientCannotSend()
    {
        var options = new WorkflowClientOptions();
        var invoker = new RecordingInvoker(_ => new StartProcessResponse
            { ProcessInstanceId = Guid.NewGuid().ToString() });
        var client = Client(invoker, options);
        options.JsonSerializerOptions.PropertyNamingPolicy = null;
        await client.StartAsync(Guid.NewGuid(), new { OrderId = 42 }, "order", Guid.NewGuid());
        Assert.Equal("{\"orderId\":42}", ((StartProcessRequest)invoker.Requests[0].Request).InputData);
        client.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.GetStatusAsync(Guid.NewGuid()));
        Assert.Single(invoker.Requests);
    }

    [Fact]
    public void DependencyInjection_ValidatesConfigurationAndReusesSingleton()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddWorkflowSchedulerClient(_ => { }));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddWorkflowSchedulerClient(options =>
        {
            options.Address = new Uri("http://localhost");
            options.RequestTimeout = TimeSpan.Zero;
        }));
        services.AddWorkflowSchedulerClient(options => options.Address = new Uri("http://localhost:5010"));
        using var provider = services.BuildServiceProvider();
        Assert.Same(
            provider.GetRequiredService<IWorkflowSchedulerClient>(),
            provider.GetRequiredService<IWorkflowSchedulerClient>());
    }

    private static WorkflowSchedulerClient Client(RecordingInvoker invoker, WorkflowClientOptions? options = null)
    {
        return new WorkflowSchedulerClient(new Processes.ProcessesClient(invoker), options);
    }

    private sealed class RecordingInvoker(Func<string, object> respond) : CallInvoker
    {
        public List<(object Request, CallOptions Options)> Requests { get; } = [];

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            Requests.Add((request, options));
            Task<TResponse> response;
            try
            {
                response = Task.FromResult((TResponse)respond(method.Name));
            }
            catch (Exception error)
            {
                response = Task.FromException<TResponse>(error);
            }

            return new AsyncUnaryCall<TResponse>(
                response,
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => [],
                () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            throw new NotSupportedException();
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options)
        {
            throw new NotSupportedException();
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request)
        {
            throw new NotSupportedException();
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options)
        {
            throw new NotSupportedException();
        }
    }
}