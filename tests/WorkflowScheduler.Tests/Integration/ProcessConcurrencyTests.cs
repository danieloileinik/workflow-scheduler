using WorkflowScheduler.Common;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class ProcessConcurrencyTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CancelAndComplete_SerializeTransitionsAndScheduleCompensationOnce(bool cancelFirst)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var undo = TestWorkflow.Call(service: "refund");
        var next = TestWorkflow.Call(service: "shipping");
        var first = TestWorkflow.Call(next.Id, undo.Id);
        var process = await app.Start(await app.Register(first, next, undo));
        var initial = await app.Send(new GetProcessStatusQuery(process));
        var callback = new CompleteStepCommand(
            process,
            first.Id,
            true,
            "{}",
            "completion",
            initial!.CurrentStepId!.Value);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var scope = app.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync(timeout.Token);
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"SELECT pg_advisory_xact_lock({AdvisoryLock.ForProcess(process)})",
            timeout.Token);

        IRequest<ErrorOr<Success>> cancel = new CancelProcessCommand(process);

        // Act: queue both operations behind the same lock in a known order.
        var firstTask = app.Send(cancelFirst ? cancel : callback, timeout.Token);
        await WaitForBlockedCommands(app, 1);

        var secondTask = app.Send(cancelFirst ? callback : cancel, timeout.Token);
        await WaitForBlockedCommands(app, 2);
        await transaction.CommitAsync(timeout.Token);

        var results = await Task.WhenAll(firstTask, secondTask);
        var status = await app.Send(new GetProcessStatusQuery(process), timeout.Token);

        // Assert
        Assert.False(results[0].IsError);
        Assert.Equal(cancelFirst, results[1].IsError);
        Assert.Equal("Cancelled", status!.Status);

        string[] expectedStates = cancelFirst ? ["Cancelled"] : ["Completed", "Cancelled", "Running"];
        Assert.Equal(expectedStates, status.StepHistory.Select(step => step.Status));
        Assert.Equal(cancelFirst ? 1 : 3, await app.WithDb(context => context.OutboxMessages.CountAsync()));
        Assert.Equal(cancelFirst ? 0 : 1, await app.WithDb(context => context.EventLogs.CountAsync()));
        Assert.Equal(cancelFirst ? 0 : 1, await app.WithDb(context => context.SagaCompensationLogs.CountAsync()));
        Assert.Equal(
            0,
            await app.WithDb(context => context.StepInstances.CountAsync(step => step.NextRetryAt != null)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CancelAndDueRetry_SerializeWithoutRevivingCancelledWork(bool cancelFirst)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.Complete(process, call.Id, false);
        await app.WithDb(context => context
            .StepInstances
            .Where(step => step.ProcessInstanceId == process)
            .ExecuteUpdateAsync(update => update.SetProperty(
                step => step.NextRetryAt,
                DateTime.UtcNow.AddSeconds(-1))));

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var scope = app.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync(timeout.Token);
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"SELECT pg_advisory_xact_lock({AdvisoryLock.ForProcess(process)})",
            timeout.Token);

        IRequest<ErrorOr<Success>> cancel = new CancelProcessCommand(process);
        IRequest<ErrorOr<Success>> retry = new RetryStepsCommand(process);

        // Act
        var first = app.Send(cancelFirst ? cancel : retry, timeout.Token);
        await WaitForBlockedCommands(app, 1);

        var second = app.Send(cancelFirst ? retry : cancel, timeout.Token);
        await WaitForBlockedCommands(app, 2);
        await transaction.CommitAsync(timeout.Token);

        var results = await Task.WhenAll(first, second);
        var status = await app.Send(new GetProcessStatusQuery(process), timeout.Token);

        // Assert
        Assert.All(results, result => Assert.False(result.IsError));
        Assert.Equal("Cancelled", status!.Status);
        Assert.Equal("Cancelled", Assert.Single(status.StepHistory).Status);
        Assert.Equal(cancelFirst ? 1 : 2, await app.WithDb(context => context.OutboxMessages.CountAsync()));
        Assert.Equal(
            0,
            await app.WithDb(context => context.StepInstances.CountAsync(step => step.NextRetryAt != null)));
        Assert.Equal(0, await app.WithDb(context => context.SagaCompensationLogs.CountAsync()));
    }

    [Fact]
    public async Task LockedProcess_DoesNotBlockAnotherProcessCallback()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var definition = await app.Register(call);
        var blockedProcess = await app.Start(definition);
        var independentProcess = await app.Start(definition);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var scope = app.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync(timeout.Token);
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"SELECT pg_advisory_xact_lock({AdvisoryLock.ForProcess(blockedProcess)})",
            timeout.Token);

        var blocked = app.Complete(blockedProcess, call.Id);
        await WaitForBlockedCommands(app, 1);

        // Act
        ErrorOr<Success> independent;

        try
        {
            independent = await app.Complete(independentProcess, call.Id).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(blocked.IsCompleted);
        }
        finally
        {
            await transaction.CommitAsync(timeout.Token);
            await blocked.WaitAsync(timeout.Token);
        }

        // Assert
        Assert.False(independent.IsError);
        Assert.False((await blocked).IsError);
        Assert.Equal("Completed", (await app.Send(new GetProcessStatusQuery(independentProcess)))!.Status);
        Assert.Equal("Completed", (await app.Send(new GetProcessStatusQuery(blockedProcess)))!.Status);
    }

    private static Task<int> WaitForBlockedCommands(TestHarness app, int count)
    {
        return Eventually.UntilAsync(
            ct => app.WithDb(db => db
                .Database
                .SqlQueryRaw<int>(
                    "SELECT count(*)::integer AS \"Value\" FROM pg_locks WHERE locktype = 'advisory' AND NOT granted")
                .SingleAsync(ct)),
            actual => actual == count,
            $"Expected {count} commands waiting for the process lock");
    }
}