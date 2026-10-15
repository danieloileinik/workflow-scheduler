using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class ConcurrencyTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task ConcurrentStarts_WithSameKey_CommitOneProcessReceiptAndOutboxMessage()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var definition = await app.Register(TestWorkflow.Call());
        var command = new StartProcessCommand(definition, Guid.NewGuid(), "order", "{}");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        // Act: Send creates an independent DI scope, DbContext and transaction for each request.
        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => app.Send(command, timeout.Token)));

        // Assert
        Assert.All(results, result => Assert.False(result.IsError));
        Assert.Single(results.Select(result => result.Value.ProcessInstanceId).Distinct());
        Assert.Equal(1, await app.WithDb(db => db.ProcessInstances.CountAsync(timeout.Token)));
        Assert.Equal(1, await app.WithDb(db => db.StartRequests.CountAsync(timeout.Token)));
        Assert.Equal(1, await app.WithDb(db => db.StepInstances.CountAsync(timeout.Token)));
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync(timeout.Token)));
    }

    [Fact]
    public async Task ConcurrentStarts_WithConflictingPayloads_OnlyAcceptWinningFingerprint()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var definition = await app.Register(TestWorkflow.Call());
        var command = new StartProcessCommand(definition, Guid.NewGuid(), "order-a", "{}");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        // Act
        var results = await Task.WhenAll(
            Enumerable
                .Range(0, 12)
                .Select(index =>
                    app.Send(command with { BusinessKey = index % 2 == 0 ? "order-a" : "order-b" }, timeout.Token)));

        // Assert
        var accepted = results.Where(result => !result.IsError).ToArray();
        var rejected = results.Where(result => result.IsError).ToArray();
        Assert.Equal(6, accepted.Length);
        Assert.Equal(6, rejected.Length);
        Assert.Single(accepted.Select(result => result.Value.ProcessInstanceId).Distinct());
        Assert.All(rejected, result => Assert.Equal("Process.IdempotencyKeyConflict", result.FirstError.Code));
        Assert.Equal(1, await app.WithDb(db => db.ProcessInstances.CountAsync(timeout.Token)));
        Assert.Equal(1, await app.WithDb(db => db.StartRequests.CountAsync(timeout.Token)));
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync(timeout.Token)));
    }

    [Fact]
    public async Task ConcurrentCallbacks_AdvanceExactlyOnceAndPersistOneEvent()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var second = TestWorkflow.Call(service: "shipping");
        var first = TestWorkflow.Call(second.Id);
        var process = await app.Start(await app.Register(first, second));
        var status = await app.Send(new GetProcessStatusQuery(process));
        var callback = new CompleteStepCommand(
            process,
            first.Id,
            true,
            "{}",
            "same-callback",
            status!.StepHistory.Single().Id);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => app.Send(callback, timeout.Token)));

        // Assert
        Assert.Single(results, result => !result.IsError);
        Assert.All(
            results.Where(result => result.IsError),
            result =>
                Assert.Equal("Process.DuplicateEvent", result.FirstError.Code));
        Assert.Equal(1, await app.WithDb(db => db.EventLogs.CountAsync(timeout.Token)));
        Assert.Equal(2, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        var persisted = await app.Send(new GetProcessStatusQuery(process), timeout.Token);
        Assert.Equal([first.Id, second.Id], persisted!.StepHistory.Select(step => step.StepDefinitionId));
        Assert.Equal(["Completed", "Running"], persisted.StepHistory.Select(step => step.Status));
    }

    [Fact]
    public async Task ConcurrentRegistrations_WithSameVersion_CommitOneDefinition()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var command = TestWorkflow.Register(TestWorkflow.Call());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => app.Send(command, timeout.Token)));

        // Assert
        Assert.Single(results, result => !result.IsError);
        Assert.All(
            results.Where(result => result.IsError),
            result =>
                Assert.Equal("ProcessDefinition.VersionConflict", result.FirstError.Code));
        Assert.Equal(1, await app.WithDb(db => db.ProcessDefinitions.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.StepDefinitions.CountAsync()));
    }
}