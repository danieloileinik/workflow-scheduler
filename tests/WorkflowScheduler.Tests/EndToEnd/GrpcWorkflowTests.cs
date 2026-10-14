using Grpc.Core;
using StackExchange.Redis;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Grpc;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.EndToEnd;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "EndToEnd")]
public class GrpcWorkflowTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task TwoDomainWorkflow_RunsThroughGrpcOutboxAndCallbacks()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        using var broker = infrastructure.CreateProbe("billing", "shipping");
        var client = app.Client;
        var shipping = TestWorkflow.Call(service: "shipping");
        var approval = TestWorkflow.Wait(shipping.Id);
        var billing = TestWorkflow.Call(approval.Id);
        var registered = await client.RegisterProcessDefinitionAsync(GrpcApp.Registration(billing, approval, shipping));
        var startRequest = new StartProcessRequest
        {
            DefinitionId = registered.DefinitionId, BusinessKey = "order-42", InputData = "{\"amount\":42}",
            IdempotencyKey = Guid.NewGuid().ToString()
        };
        // Act
        var started = await client.StartProcessAsync(startRequest);
        var replay = await client.StartProcessAsync(startRequest);
        var processId = Guid.Parse(started.ProcessInstanceId);
        var billingCommand = await broker.ReadAsync("billing", processId);
        await client.CompleteStepAsync(Callback(billingCommand));
        var waiting = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest { ProcessInstanceId = started.ProcessInstanceId });
        await client.SendEventAsync(
            new SendEventRequest
            {
                ProcessInstanceId = started.ProcessInstanceId, EventName = "Approved", CorrelationId = "approval-42",
                Payload = "{}"
            });
        var shippingCommand = await broker.ReadAsync("shipping", processId);
        await client.CompleteStepAsync(Callback(shippingCommand));
        var status = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest { ProcessInstanceId = started.ProcessInstanceId });
        await app.WaitForOutboxAsync(2);
        // Assert
        Assert.Equal(started.ProcessInstanceId, replay.ProcessInstanceId);
        Assert.Equal("Waiting", waiting.Status);
        Assert.Equal("Waiting", waiting.StepHistory.Last().Status);
        Assert.Equal(billing.Id, billingCommand.StepDefinitionId);
        Assert.Equal(shipping.Id, shippingCommand.StepDefinitionId);
        Assert.Equal(0, broker.CountRemaining(processId));
        Assert.Equal(billingCommand.InputData, shippingCommand.InputData);
        Assert.Equal("Completed", status.Status);
        Assert.Equal(3, status.StepHistory.Count);
        Assert.All(status.StepHistory, x => Assert.Equal("Completed", x.Status));
    }

    [Theory]
    [InlineData("not-a-guid")]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    public async Task InvalidOptionalStepReference_ReturnsInvalidArgument(string reference)
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        var request = GrpcApp.Registration(TestWorkflow.Call());
        request.Steps[0].NextStepId = reference;
        // Act
        var exception =
            await Assert.ThrowsAsync<RpcException>(async () =>
                await app.Client.RegisterProcessDefinitionAsync(request));
        // Assert
        Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);
    }

    [Fact]
    public async Task MissingProcess_ReturnsNotFound()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        var request = new GetProcessStatusRequest { ProcessInstanceId = Guid.NewGuid().ToString() };
        // Act
        var exception =
            await Assert.ThrowsAsync<RpcException>(async () => await app.Client.GetProcessStatusAsync(request));
        // Assert
        Assert.Equal(StatusCode.NotFound, exception.StatusCode);
    }

    [Fact]
    public async Task BlankCorrelationId_ReturnsInvalidArgument()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        var request = new SendEventRequest { ProcessInstanceId = Guid.NewGuid().ToString(), EventName = "Approved" };
        // Act
        var exception = await Assert.ThrowsAsync<RpcException>(async () => await app.Client.SendEventAsync(request));
        // Assert
        Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);
    }

    [Fact]
    public async Task CancellationThroughGrpc_RejectsLateCallbackAndRepeatedCancellation()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var definition = await app.Client.RegisterProcessDefinitionAsync(GrpcApp.Registration(TestWorkflow.Call()));
        var started = await app.Client.StartProcessAsync(
            new StartProcessRequest
            {
                DefinitionId = definition.DefinitionId, IdempotencyKey = Guid.NewGuid().ToString(),
                BusinessKey = "order", InputData = "{}"
            });
        var command = await broker.ReadAsync("billing", Guid.Parse(started.ProcessInstanceId));
        var cancel = new CancelProcessRequest { ProcessInstanceId = started.ProcessInstanceId };
        // Act
        await app.Client.CancelProcessAsync(cancel);
        var repeated = await Assert.ThrowsAsync<RpcException>(async () => await app.Client.CancelProcessAsync(cancel));
        var late = await Assert.ThrowsAsync<RpcException>(async () =>
            await app.Client.CompleteStepAsync(Callback(command)));
        // Assert
        Assert.Equal(StatusCode.FailedPrecondition, repeated.StatusCode);
        Assert.Equal(StatusCode.FailedPrecondition, late.StatusCode);
    }

    [Fact]
    public async Task FailedCallback_RetryWorkerPublishesNextAttemptAndRejectsOldAttempt()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var client = app.Client;
        var call = TestWorkflow.Call();
        var definition = await client.RegisterProcessDefinitionAsync(GrpcApp.Registration(call));
        var started = await client.StartProcessAsync(
            new StartProcessRequest
            {
                DefinitionId = definition.DefinitionId, IdempotencyKey = Guid.NewGuid().ToString(),
                BusinessKey = "retry-order", InputData = "{}"
            });
        var processId = Guid.Parse(started.ProcessInstanceId);
        var initial = await broker.ReadAsync("billing", processId);
        var failure = Callback(initial);
        failure.Success = false;
        failure.OutputData = "temporary failure";

        // Act
        await client.CompleteStepAsync(failure);
        var pending = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest
                { ProcessInstanceId = processId.ToString() });
        // Advance the persisted deadline; the real RetryWorker must discover and dispatch it.
        await app.WithDb(async db =>
        {
            var step = await db.StepInstances.SingleAsync(step => step.Id == initial.StepInstanceId);
            db.Entry(step).Property(step => step.NextRetryAt).CurrentValue = DateTime.UtcNow.AddSeconds(-1);
            return await db.SaveChangesAsync();
        });
        var retry = await broker.ReadAsync("billing", processId);
        var stale = await Assert.ThrowsAsync<RpcException>(async () =>
            await client.CompleteStepAsync(Callback(initial)));
        await client.CompleteStepAsync(Callback(retry));
        var completed = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest
                { ProcessInstanceId = processId.ToString() });
        await app.WaitForOutboxAsync(2);

        // Assert
        Assert.Equal("Pending", pending.StepHistory.Single().Status);
        Assert.Equal(initial.StepInstanceId, retry.StepInstanceId);
        Assert.Equal(initial.CorrelationId, retry.CorrelationId);
        Assert.Equal(0, initial.Attempt);
        Assert.Equal(1, retry.Attempt);
        Assert.Equal(StatusCode.FailedPrecondition, stale.StatusCode);
        Assert.Equal("Completed", completed.Status);
        Assert.Equal(1, completed.StepHistory.Single().RetryCount);
        Assert.Equal(2, await app.WithDb(db => db.EventLogs.CountAsync()));
        Assert.Equal(0, broker.CountRemaining(processId));
    }

    [Fact]
    public async Task Cancellation_PublishesCompensationsInReverseOrderThroughKafka()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        using var broker = infrastructure.CreateProbe("billing", "shipping", "undo-a", "undo-b");
        var client = app.Client;
        var undoBilling = TestWorkflow.Call(service: "undo-a");
        var undoShipping = TestWorkflow.Call(service: "undo-b");
        var wait = TestWorkflow.Wait();
        var shipping = TestWorkflow.Call(wait.Id, undoShipping.Id, "shipping");
        var billing = TestWorkflow.Call(shipping.Id, undoBilling.Id);
        var definition = await client.RegisterProcessDefinitionAsync(
            GrpcApp.Registration(billing, shipping, wait, undoBilling, undoShipping));
        var started = await client.StartProcessAsync(
            new StartProcessRequest
            {
                DefinitionId = definition.DefinitionId, IdempotencyKey = Guid.NewGuid().ToString(),
                BusinessKey = "cancel-order", InputData = "{}"
            });
        var processId = Guid.Parse(started.ProcessInstanceId);
        var billed = await broker.ReadAsync("billing", processId);
        await client.CompleteStepAsync(Callback(billed));
        var shipped = await broker.ReadAsync("shipping", processId);
        await client.CompleteStepAsync(Callback(shipped));

        // Act
        await client.CancelProcessAsync(new CancelProcessRequest { ProcessInstanceId = processId.ToString() });
        var undoSecond = await broker.ReadAsync("undo-b", processId);
        var midway = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest
                { ProcessInstanceId = processId.ToString() });
        await client.CompleteStepAsync(Callback(undoSecond));
        var undoFirst = await broker.ReadAsync("undo-a", processId);
        await client.CompleteStepAsync(Callback(undoFirst));
        var completed = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest
                { ProcessInstanceId = processId.ToString() });
        await app.WaitForOutboxAsync(4);

        // Assert
        Assert.Equal(undoShipping.Id.ToString(), midway.StepHistory.Last().StepDefinitionId);
        Assert.DoesNotContain(midway.StepHistory, step => step.StepDefinitionId == undoBilling.Id.ToString());
        Assert.True(undoSecond.IsCompensation);
        Assert.True(undoFirst.IsCompensation);
        Assert.NotEqual(shipped.StepInstanceId, undoSecond.StepInstanceId);
        Assert.NotEqual(billed.StepInstanceId, undoFirst.StepInstanceId);
        Assert.Equal("Cancelled", completed.Status);
        Assert.Equal(["Compensated", "Compensated"], completed.StepHistory.Take(2).Select(step => step.Status));
        Assert.Equal(
            [undoShipping.Id.ToString(), undoBilling.Id.ToString()],
            completed.StepHistory.TakeLast(2).Select(step => step.StepDefinitionId));
        Assert.Equal(
            2,
            await app.WithDb(db => db.SagaCompensationLogs.CountAsync(log =>
                log.Status == CompensationStatus.Done)));
        Assert.Equal(0, broker.CountRemaining(processId));
    }

    [Fact]
    public async Task ConcurrentGrpcStarts_UseDurableIdempotencyRedisCacheAndKafkaWorkers()
    {
        // Arrange
        await using var app = await infrastructure.CreateGrpcAppAsync();
        using var broker = infrastructure.CreateProbe("billing", "shipping");
        var client = app.Client;
        var shipping = TestWorkflow.Call(service: "shipping");
        var billing = TestWorkflow.Call(shipping.Id);
        var definition = await client.RegisterProcessDefinitionAsync(GrpcApp.Registration(billing, shipping));
        var request = new StartProcessRequest
        {
            DefinitionId = definition.DefinitionId, BusinessKey = "concurrent-order", InputData = "{}",
            IdempotencyKey = Guid.NewGuid().ToString()
        };

        // Act
        var starts = await Task.WhenAll(
            Enumerable.Range(0, 12).Select(async _ => await client.StartProcessAsync(request)));
        var processId = Guid.Parse(starts[0].ProcessInstanceId);
        var first = await broker.ReadAsync("billing", processId);
        await client.CompleteStepAsync(Callback(first));
        var second = await broker.ReadAsync("shipping", processId);
        await client.CompleteStepAsync(Callback(second));
        var status = await client.GetProcessStatusAsync(
            new GetProcessStatusRequest { ProcessInstanceId = processId.ToString() });
        await app.WaitForOutboxAsync(2);
        await using var scope = app.Services.CreateAsyncScope();
        var cache = scope.ServiceProvider.GetRequiredService<IProcessDefinitionCache>();
        var cached = await cache.GetAsync(Guid.Parse(definition.DefinitionId));
        var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var ttl = await redis.KeyTimeToLiveAsync($"proc-def:v2:{definition.DefinitionId}");

        // Assert
        Assert.Single(starts.Select(start => start.ProcessInstanceId).Distinct());
        Assert.Equal("Completed", status.Status);
        Assert.Equal(billing.Id, first.StepDefinitionId);
        Assert.Equal(shipping.Id, second.StepDefinitionId);
        Assert.Equal(first.ProcessInstanceId, second.ProcessInstanceId);
        Assert.Equal(0, broker.CountRemaining(processId));
        Assert.Equal(1, await app.WithDb(db => db.ProcessInstances.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.StartRequests.CountAsync()));
        Assert.Equal(2, await app.WithDb(db => db.OutboxMessages.CountAsync(message => message.Error == null)));
        Assert.NotNull(cached);
        Assert.Equal(billing.Id, cached.GetFirstStep().Value.Id);
        Assert.Equal([billing.Id, shipping.Id], cached.Steps.Select(step => step.Id));
        Assert.NotNull(ttl);
        Assert.InRange(ttl.Value, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));
    }

    private static CompleteStepRequest Callback(StepCommandEvent command)
    {
        return new CompleteStepRequest
        {
            ProcessInstanceId = command.ProcessInstanceId.ToString(),
            StepDefinitionId = command.StepDefinitionId.ToString(),
            StepInstanceId = command.StepInstanceId.ToString(), Attempt = command.Attempt,
            CorrelationId = Guid.NewGuid().ToString(), Success = true, OutputData = "{}"
        };
    }
}