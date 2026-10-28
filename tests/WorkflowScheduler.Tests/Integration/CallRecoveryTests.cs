using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using WorkflowScheduler.Contracts.Grpc;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class CallRecoveryTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task MissingCallback_ConcurrentRecoveryWorkersRedeliverTheSameAttemptOnce()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var process = await app.Start(await app.Register(TestWorkflow.Call()));
        await app.Dispatch();
        var before = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.True(before.NextRecoveryAt > DateTime.UtcNow);
        await ExpireRecovery(app, process);
        var worker = new RetryWorker(app.Services.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<RetryWorker>.Instance);

        // Act
        await Task.WhenAll(worker.DispatchAsync(CancellationToken.None), worker.DispatchAsync(CancellationToken.None));
        var queued = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        var queuedMessages = await app.WithDb(db => db.OutboxMessages.CountAsync());
        var subsequent = await worker.DispatchAsync(CancellationToken.None);
        await app.Dispatch();

        // Assert
        Assert.Equal(2, queuedMessages);
        Assert.Equal(0, subsequent);
        Assert.Null(queued.NextRecoveryAt);
        Assert.Equal(before.Id, queued.Id);
        Assert.Equal(before.RetryCount, queued.RetryCount);
        Assert.Equal(StepStatus.Running, queued.Status);
        Assert.Equal(2, app.Publisher.Messages.Count);
        Assert.Equal(ReadCommand(app, 0), ReadCommand(app, 1));
        var delivered = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.True(delivered.NextRecoveryAt > DateTime.UtcNow);
    }

    [Fact]
    public async Task CallbackBeforeRecovery_ClearsDeadlineAndPreventsRedelivery()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.Dispatch();
        await ExpireRecovery(app, process);

        // Act
        var completion = await app.Complete(process, call.Id);
        var recovery = await app.Send(new RetryStepsCommand(process));

        // Assert
        Assert.False(completion.IsError);
        Assert.False(recovery.IsError);
        var step = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.Equal(StepStatus.Completed, step.Status);
        Assert.Null(step.NextRecoveryAt);
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
    }

    [Fact]
    public async Task CallbackAfterRecoveryQueued_CompletesOriginalAttemptAndSuppressesPendingDelivery()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.Dispatch();
        await ExpireRecovery(app, process);
        Assert.False((await app.Send(new RetryStepsCommand(process))).IsError);

        // Act
        var completion = await app.Complete(process, call.Id);
        await app.Dispatch();

        // Assert
        Assert.False(completion.IsError);
        Assert.Single(app.Publisher.Messages);
        Assert.Equal(2, await app.WithDb(db => db.OutboxMessages.CountAsync(message => message.ProcessedAt != null)));
        var step = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.Equal(StepStatus.Completed, step.Status);
        Assert.Null(step.NextRecoveryAt);
    }

    [Fact]
    public async Task BrokerUnavailable_DoesNotAccumulateRecoveryMessagesBeforeDelivery()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var process = await app.Start(await app.Register(TestWorkflow.Call()));
        await app.Dispatch();
        await ExpireRecovery(app, process);
        Assert.False((await app.Send(new RetryStepsCommand(process))).IsError);
        app.Publisher.Fail = true;

        // Act
        await app.Dispatch();
        await app.Send(new RetryStepsCommand(process));
        await app.Send(new RetryStepsCommand(process));

        // Assert
        Assert.Equal(2, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Single(app.Publisher.Messages);
        var step = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Equal(0, step.RetryCount);
        Assert.Null(step.NextRecoveryAt);
    }

    [Fact]
    public async Task MissingCompensationCallback_RedeliversWhileProcessRemainsCancelled()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var undo = TestWorkflow.Call(service: "refund");
        var wait = TestWorkflow.Wait();
        var call = TestWorkflow.Call(wait.Id, undo.Id);
        var process = await app.Start(await app.Register(call, wait, undo));
        await app.Complete(process, call.Id);
        await app.Send(new CancelProcessCommand(process));
        await app.Dispatch();
        await ExpireRecovery(app, process);

        // Act
        var result = await app.Send(new RetryStepsCommand(process));
        await app.Dispatch();

        // Assert
        Assert.False(result.IsError);
        var state = await app.Send(new GetProcessStatusQuery(process));
        Assert.Equal("Cancelled", state!.Status);
        Assert.Equal(3, state.TotalStepCount);
        Assert.Equal(2, app.Publisher.Messages.Count);
        Assert.Equal(ReadCommand(app, 0), ReadCommand(app, 1));
        Assert.True(ReadCommand(app, 1).IsCompensation);
        var step = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync(candidate => candidate.IsCompensation));
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Equal(0, step.RetryCount);
    }

    [Fact]
    public async Task ExplicitFailure_ClearsRecoveryDeadlineAndPreservesBusinessRetryAttemptChecks()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.Dispatch();

        // Act
        var failure = await app.Complete(process, call.Id, false);
        var pending = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        await app.WithDb(db => db.StepInstances.ExecuteUpdateAsync(update =>
            update.SetProperty(step => step.NextRetryAt, DateTime.UtcNow.AddSeconds(-1))));
        var retry = await app.Send(new RetryStepsCommand(process));
        await app.Dispatch();
        var stale = await app.Complete(process, call.Id, attempt: 0);
        var completion = await app.Complete(process, call.Id, attempt: 1);

        // Assert
        Assert.False(failure.IsError);
        Assert.Null(pending.NextRecoveryAt);
        Assert.Equal(StepStatus.Pending, pending.Status);
        Assert.Equal(1, pending.RetryCount);
        Assert.False(retry.IsError);
        Assert.True(stale.IsError);
        Assert.Equal("ProcessStep.StaleCallback", stale.FirstError.Code);
        Assert.False(completion.IsError);
        Assert.Equal(ReadCommand(app, 0).StepInstanceId, ReadCommand(app, 1).StepInstanceId);
        Assert.Equal(0, ReadCommand(app, 0).Attempt);
        Assert.Equal(1, ReadCommand(app, 1).Attempt);
        var finished = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.Null(finished.NextRecoveryAt);
    }

    [Fact]
    public async Task DatabaseUpgrade_ArmsLegacyRunningCallsOnlyOnce()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        await app.WithDb(db => db.Database.ExecuteSqlRawAsync(
            "ALTER TABLE step_instances DROP COLUMN \"NextRecoveryAt\""));

        // Act
        var restored = await app.WithDb(async db =>
        {
            await DatabaseInitializer.InitializeAsync(db);

            return await db.StepInstances.AsNoTracking().SingleAsync();
        });
        await app.WithDb(async db =>
        {
            await db.StepInstances.ExecuteUpdateAsync(update => update.SetProperty(step => step.NextRecoveryAt, (DateTime?)null));
            await DatabaseInitializer.InitializeAsync(db);

            return 0;
        });

        // Assert
        Assert.True(restored.NextRecoveryAt > DateTime.UtcNow);
        var pending = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync());
        Assert.Equal(restored.Id, pending.Id);
        Assert.Null(pending.NextRecoveryAt);
    }

    private static Task<int> ExpireRecovery(TestHarness app, Guid process)
    {
        return app.WithDb(db => db.StepInstances
            .Where(step => step.ProcessInstanceId == process && step.Status == StepStatus.Running)
            .ExecuteUpdateAsync(update => update.SetProperty(step => step.NextRecoveryAt, DateTime.UtcNow.AddSeconds(-1))));
    }

    private static ExecuteStepCommand ReadCommand(TestHarness app, int index)
    {
        return JsonSerializer.Deserialize<ExecuteStepCommand>(app.Publisher.Messages[index].Payload)!;
    }
}
