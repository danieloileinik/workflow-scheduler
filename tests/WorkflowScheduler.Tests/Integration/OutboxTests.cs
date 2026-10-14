using System.Text.Json;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class OutboxTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task PublisherFailure_PreservesMessageAndRetriesWithSameIdentity()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var process = await app.Start(await app.Register(TestWorkflow.Call()));
        app.Publisher.Fail = true;
        // Act
        await app.Dispatch();
        var error = await app.WithDb(db => db.OutboxMessages.Select(x => x.Error).SingleAsync());
        app.Publisher.Fail = false;
        await app.Dispatch();
        await app.Dispatch();
        // Assert
        Assert.Equal(2048, error!.Length);
        Assert.Single(app.Publisher.Messages);
        Assert.Equal(process.ToString(), app.Publisher.Messages[0].Key);
        var command = JsonSerializer.Deserialize<StepCommandEvent>(app.Publisher.Messages[0].Payload);
        Assert.Equal(process, command!.ProcessInstanceId);
        Assert.Equal(command.StepInstanceId.ToString(), command.CorrelationId);
        Assert.Equal(0, command.Attempt);
        var delivered = await broker.ReadAsync("billing", process);
        Assert.Equal(command, delivered);
        Assert.Equal(0, broker.CountRemaining(process));
        Assert.Equal(
            1,
            await app.WithDb(db => db.OutboxMessages.CountAsync(x => x.ProcessedAt != null && x.Error == null)));
    }

    [Fact]
    public async Task Cancellation_SuppressesUnsentForwardCommand()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var process = await app.Start(await app.Register(TestWorkflow.Call()));
        // Act
        await app.Send(new CancelProcessCommand(process));
        await app.Dispatch();
        // Assert
        Assert.Empty(app.Publisher.Messages);
        Assert.Equal(0, broker.CountRemaining(process));
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync(x => x.ProcessedAt != null)));
    }

    [Fact]
    public async Task ConcurrentDispatchers_SkipLockedBatchAndPublishEachCommandOnce()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var definition = await app.Register(TestWorkflow.Call());
        var processes = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => app.Start(definition)));
        var publishing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        app.Publisher.BeforePublishAsync = async ct =>
        {
            publishing.TrySetResult();
            await release.Task.WaitAsync(ct);
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var owner = app.Dispatch(timeout.Token);
        Task<int>? competitor = null;

        // Act: keep the first dispatch transaction open while the second claims its batch.
        try
        {
            await publishing.Task.WaitAsync(TimeSpan.FromSeconds(10));
            competitor = app.Dispatch(timeout.Token);
            Assert.Equal(0, await competitor.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.False(owner.IsCompleted);
            Assert.Empty(app.Publisher.Messages);
        }
        finally
        {
            release.TrySetResult();
            await Task.WhenAll(owner, competitor ?? Task.FromResult(0));
            app.Publisher.BeforePublishAsync = null;
        }

        // Assert
        Assert.Equal(4, await owner);
        Assert.Equal(4, app.Publisher.Messages.Count);
        foreach (var process in processes)
        {
            var command = await broker.ReadAsync("billing", process);
            Assert.Equal(process, command.ProcessInstanceId);
            Assert.Equal(0, broker.CountRemaining(process));
        }

        Assert.Equal(4, await app.WithDb(db => db.OutboxMessages.CountAsync(message => message.ProcessedAt != null)));
    }
}