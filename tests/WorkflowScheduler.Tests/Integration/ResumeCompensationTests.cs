using WorkflowScheduler.Features.Processes.Commands.ResumeCompensation;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class ResumeCompensationTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Resume_PreservesFailedExecutionAndContinuesReverseCompensationOrder(bool failedProcess)
    {
        // Arrange
        var (app, process, failedId, head, tail) = await CreateFailedCompensation(failedProcess);
        var before = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync(step => step.Id == failedId));

        // Act
        var result = await app.Send(new ResumeCompensationCommand(process, failedId));
        var running = await app.WithDb(db => db.SagaCompensationLogs.AsNoTracking()
            .SingleAsync(log => log.ProcessInstanceId == process && log.Status == CompensationStatus.Running));
        var resumed = await app.WithDb(db => db.StepInstances.AsNoTracking()
            .SingleAsync(step => step.Id == running.StepInstanceId));
        var late = await app.Send(new CompleteStepCommand(process, head, true, "{}", "old-callback", failedId,
            before.RetryCount));
        var headCompletion = await app.Complete(process, head);
        var tailCompletion = await app.Complete(process, tail);

        // Assert
        Assert.False(result.IsError);
        Assert.NotEqual(failedId, resumed.Id);
        Assert.Equal(0, resumed.RetryCount);
        Assert.Equal(StepStatus.Running, resumed.Status);
        Assert.True(resumed.Sequence > before.Sequence);
        Assert.Null(running.Error);
        Assert.Null(running.CompletedAt);
        Assert.True(late.IsError);
        Assert.False(headCompletion.IsError);
        Assert.False(tailCompletion.IsError);
        var original = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync(step => step.Id == failedId));
        Assert.Equal(StepStatus.Failed, original.Status);
        Assert.Equal(before.Error, original.Error);
        Assert.Equal(before.CompletedAt, original.CompletedAt);
        var logs = await app.WithDb(db => db.SagaCompensationLogs.AsNoTracking().OrderBy(log => log.Sequence).ToArrayAsync());
        Assert.Equal([head, tail], logs.Select(log => log.StepDefinitionId));
        Assert.All(logs, log => Assert.Equal(CompensationStatus.Done, log.Status));
        var state = await app.Send(new GetProcessStatusQuery(process));
        Assert.Equal(failedProcess ? "Failed" : "Cancelled", state!.Status);
    }

    [Fact]
    public async Task ConcurrentResumeRequests_CreateOneExecutionOutboxMessageAndReceipt()
    {
        // Arrange
        var (app, process, failedId, _, _) = await CreateFailedCompensation();
        var command = new ResumeCompensationCommand(process, failedId);
        var steps = await app.WithDb(db => db.StepInstances.CountAsync());
        var outbox = await app.WithDb(db => db.OutboxMessages.CountAsync());

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => app.Send(command)));
        var replay = await app.Send(command);

        // Assert
        Assert.All(results, result => Assert.False(result.IsError));
        Assert.False(replay.IsError);
        Assert.Equal(steps + 1, await app.WithDb(db => db.StepInstances.CountAsync()));
        Assert.Equal(outbox + 1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "ResumeCompensation")));
        Assert.Equal(1, await app.WithDb(db => db.SagaCompensationLogs.CountAsync(log => log.Status == CompensationStatus.Running)));
    }

    [Fact]
    public async Task ReplayOfOldResume_DoesNotResumeALaterFailedExecution()
    {
        // Arrange
        var (app, process, failedId, head, _) = await CreateFailedCompensation();
        var original = new ResumeCompensationCommand(process, failedId);
        Assert.False((await app.Send(original)).IsError);
        var newFailedId = await FailCompensation(app, process, head);
        var outbox = await app.WithDb(db => db.OutboxMessages.CountAsync());

        // Act
        var replay = await app.Send(original);
        var afterReplay = await app.WithDb(db => db.OutboxMessages.CountAsync());
        var explicitResume = await app.Send(new ResumeCompensationCommand(process, newFailedId));

        // Assert
        Assert.False(replay.IsError);
        Assert.Equal(outbox, afterReplay);
        Assert.False(explicitResume.IsError);
        Assert.Equal(outbox + 1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Equal(2, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "ResumeCompensation")));
        Assert.Equal(2, await app.WithDb(db => db.StepInstances.CountAsync(step => step.IsCompensation && step.Status == StepStatus.Failed)));
    }

    [Fact]
    public async Task ResumeWhileCompensationIsRunning_DoesNotCreateAnotherExecution()
    {
        // Arrange
        var (app, process, failedId, _, _) = await CreateFailedCompensation();
        Assert.False((await app.Send(new ResumeCompensationCommand(process, failedId))).IsError);
        var runningId = await app.WithDb(db => db.SagaCompensationLogs
            .Where(log => log.Status == CompensationStatus.Running).Select(log => log.StepInstanceId!.Value).SingleAsync());
        var outbox = await app.WithDb(db => db.OutboxMessages.CountAsync());

        // Act
        var result = await app.Send(new ResumeCompensationCommand(process, runningId));

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.CompensationNotFailed", result.FirstError.Code);
        Assert.Equal(outbox, await app.WithDb(db => db.OutboxMessages.CountAsync()));
    }

    [Fact]
    public async Task FailedExecutionFromAnotherProcess_IsRejected()
    {
        // Arrange
        var (app, _, failedId, _, _) = await CreateFailedCompensation();
        var other = await app.Start(await app.Register(TestWorkflow.Call()));
        await app.Send(new CancelProcessCommand(other));

        // Act
        var result = await app.Send(new ResumeCompensationCommand(other, failedId));

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.NotFound", result.FirstError.Code);
        Assert.Equal(0, await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "ResumeCompensation")));
    }

    [Fact]
    public async Task RunningProcess_CannotResumeCompensation()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var process = await app.Start(await app.Register(TestWorkflow.Call()));

        // Act
        var result = await app.Send(new ResumeCompensationCommand(process, Guid.NewGuid()));

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.NotCompensating", result.FirstError.Code);
    }

    [Fact]
    public async Task OutboxInsertFailure_RollsBackResumeAndAllowsSameRequestToSucceedLater()
    {
        // Arrange
        var (app, process, failedId, _, _) = await CreateFailedCompensation();
        var command = new ResumeCompensationCommand(process, failedId);
        var steps = await app.WithDb(db => db.StepInstances.CountAsync());
        await app.WithDb(db => db.Database.ExecuteSqlRawAsync(
            """
            CREATE FUNCTION reject_resume_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'Injected resume failure'; END $$;
            CREATE TRIGGER reject_resume_outbox BEFORE INSERT ON outbox_messages
                FOR EACH ROW EXECUTE FUNCTION reject_resume_outbox();
            """));

        // Act
        try
        {
            await Assert.ThrowsAsync<DbUpdateException>(() => app.Send(command));
        }
        finally
        {
            await app.WithDb(db => db.Database.ExecuteSqlRawAsync(
                "DROP TRIGGER reject_resume_outbox ON outbox_messages; DROP FUNCTION reject_resume_outbox();"));
        }

        var afterFailure = await app.WithDb(db => db.StepInstances.CountAsync());
        var receiptsAfterFailure = await app.WithDb(db => db.EventLogs.CountAsync(log => log.EventName == "ResumeCompensation"));
        var result = await app.Send(command);

        // Assert
        Assert.Equal(steps, afterFailure);
        Assert.Equal(0, receiptsAfterFailure);
        Assert.False(result.IsError);
        Assert.Equal(steps + 1, await app.WithDb(db => db.StepInstances.CountAsync()));
    }

    private async Task<(TestHarness App, Guid Process, Guid FailedId, Guid Head, Guid Tail)> CreateFailedCompensation(
        bool failedProcess = false)
    {
        var app = await infrastructure.CreateHarnessAsync();
        var undoA = TestWorkflow.Call(service: "undo-a");
        var undoB = TestWorkflow.Call(service: "undo-b");
        var end = failedProcess ? TestWorkflow.Call(service: "shipping") : TestWorkflow.Wait();
        var b = TestWorkflow.Call(end.Id, undoB.Id);
        var a = TestWorkflow.Call(b.Id, undoA.Id);
        var process = await app.Start(await app.Register(a, b, end, undoA, undoB));
        await app.Complete(process, a.Id);
        await app.Complete(process, b.Id);

        if (failedProcess)
        {
            await app.WithDb(db => db.StepInstances.Where(step => step.ProcessInstanceId == process && step.StepDefinitionId == end.Id)
                .ExecuteUpdateAsync(update => update.SetProperty(step => step.RetryCount, step => step.MaxRetries)));
            Assert.False((await app.Complete(process, end.Id, false, 3)).IsError);
        }
        else
        {
            Assert.False((await app.Send(new CancelProcessCommand(process))).IsError);
        }

        return (app, process, await FailCompensation(app, process, undoB.Id), undoB.Id, undoA.Id);
    }

    private static async Task<Guid> FailCompensation(TestHarness app, Guid process, Guid definitionId)
    {
        await app.WithDb(db => db.StepInstances
            .Where(step => step.ProcessInstanceId == process && step.StepDefinitionId == definitionId && step.Status == StepStatus.Running)
            .ExecuteUpdateAsync(update => update.SetProperty(step => step.RetryCount, step => step.MaxRetries)));
        Assert.False((await app.Complete(process, definitionId, false, 3)).IsError);

        return await app.WithDb(db => db.SagaCompensationLogs
            .Where(log => log.ProcessInstanceId == process && log.StepDefinitionId == definitionId)
            .Select(log => log.StepInstanceId!.Value).SingleAsync());
    }
}
