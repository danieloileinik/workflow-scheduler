using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class ProcessTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task StartReplay_ReturnsOriginalIdAndRejectsDifferentPayload()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var definitionId = await app.Register(TestWorkflow.Call());
        var command = new StartProcessCommand(definitionId, Guid.NewGuid(), "order", "{}");
        // Act
        var first = await app.Send(command);
        var replay = await app.Send(command);
        var changed = await app.Send(command with { BusinessKey = "different" });
        // Assert
        Assert.Equal(first.Value.ProcessInstanceId, replay.Value.ProcessInstanceId);
        Assert.Equal("Process.IdempotencyKeyConflict", changed.FirstError.Code);
        Assert.Equal(1, await app.WithDb(db => db.ProcessInstances.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.StartRequests.CountAsync()));
    }

    [Fact]
    public async Task FailedStart_DoesNotConsumeIdempotencyKey()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var definitionId = Guid.NewGuid();
        var command = new StartProcessCommand(definitionId, Guid.NewGuid(), "order", "{}");
        // Act
        var missing = await app.Send(command);
        await app.WithDb(async db =>
        {
            var definition = new ProcessDefinition(definitionId, "late", 1);
            definition.AddStep(
                new StepDefinition(
                    definitionId,
                    "call",
                    StepType.Call,
                    """{"ServiceName":"billing","Command":"Execute"}"""));
            db.Add(definition);

            return await db.SaveChangesAsync();
        });

        var retry = await app.Send(command);
        // Assert
        Assert.Equal("ProcessDefinition.NotFound", missing.FirstError.Code);
        Assert.False(retry.IsError);
        Assert.Equal(1, await app.WithDb(db => db.StartRequests.CountAsync()));
    }

    [Fact]
    public async Task CallWaitCall_CompletesAndDeduplicatesEventsCaseInsensitively()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var last = TestWorkflow.Call(service: "shipping");
        var wait = TestWorkflow.Wait(last.Id);
        var first = TestWorkflow.Call(wait.Id);
        var definition = await app.Register(first, wait, last);
        var process = await app.Start(definition);
        // Act
        var completed = await app.Complete(process, first.Id);
        var wrong = await app.Send(new SendEventCommand(process, "Wrong", null, "event1"));
        var sent = await app.Send(new SendEventCommand(process, "approved", "{}", "event1"));
        var duplicate = await app.Send(new SendEventCommand(process, "APPROVED", "{}", "event1"));
        var finished = await app.Complete(process, last.Id);
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.False(completed.IsError);
        Assert.Equal("Process.EventNameMismatch", wrong.FirstError.Code);
        Assert.False(sent.IsError);
        Assert.Equal("Process.DuplicateEvent", duplicate.FirstError.Code);
        Assert.False(finished.IsError);
        Assert.Equal("Completed", status!.Status);
        Assert.Equal([first.Id, wait.Id, last.Id], status.StepHistory.Select(x => x.StepDefinitionId));
        Assert.All(status.StepHistory, x => Assert.Equal("Completed", x.Status));
    }

    [Fact]
    public async Task CompleteStep_CannotCompleteWaitEvent()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var wait = TestWorkflow.Wait();
        var process = await app.Start(await app.Register(wait));
        // Act
        var result = await app.Complete(process, wait.Id);
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Waiting", status!.Status);
        Assert.Equal("Waiting", status.StepHistory.Single().Status);
        Assert.Equal(0, await app.WithDb(db => db.EventLogs.CountAsync()));
    }

    [Fact]
    public async Task Retry_WaitsForDeadlineRejectsStaleAttemptAndStopsAtLimit()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        // Act
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var failure = await app.Complete(process, call.Id, false, attempt);
            Assert.False(failure.IsError);
            if (attempt == 3)
                break;

            await app.Send(new RetryStepsCommand(process));
            Assert.Equal(attempt + 1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
            await app.WithDb(async db =>
            {
                var step = await db.StepInstances.SingleAsync(x => x.ProcessInstanceId == process);
                db.Entry(step).Property(x => x.NextRetryAt).CurrentValue = DateTime.UtcNow.AddSeconds(-1);

                return await db.SaveChangesAsync();
            });
            await app.Send(new RetryStepsCommand(process));

            var stale = await app.Complete(process, call.Id, true, attempt);
            Assert.Equal("ProcessStep.StaleCallback", stale.FirstError.Code);
        }

        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.Equal("Failed", status!.Status);
        Assert.Equal(3, status.StepHistory.Single().RetryCount);
        Assert.Equal("Failed", status.StepHistory.Single().Status);
        Assert.Equal(4, await app.WithDb(db => db.OutboxMessages.CountAsync()));
    }

    [Fact]
    public async Task InvalidNextStep_RollsBackCallbackAndOutbox()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.WithDb(async db =>
        {
            var definition = await db.StepDefinitions.SingleAsync(x => x.Id == call.Id);
            db.Entry(definition).Property(x => x.NextStepId).CurrentValue = Guid.NewGuid();

            return await db.SaveChangesAsync();
        });
        await app.ClearCacheAsync();
        // Act
        var result = await app.Complete(process, call.Id);
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.Equal("ProcessStep.NotFound", result.FirstError.Code);
        Assert.Equal("Waiting", status!.Status);
        Assert.Equal("Running", status.StepHistory.Single().Status);
        Assert.Equal(0, await app.WithDb(db => db.EventLogs.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
    }

    [Fact]
    public async Task Cancel_CompensatesCompletedStepsSequentiallyInReverseOrder()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var undoA = TestWorkflow.Call(service: "undo-a");
        var undoB = TestWorkflow.Call(service: "undo-b");
        var wait = TestWorkflow.Wait();
        var b = TestWorkflow.Call(wait.Id, undoB.Id);
        var a = TestWorkflow.Call(b.Id, undoA.Id);
        var process = await app.Start(await app.Register(a, b, wait, undoA, undoB));
        await app.Complete(process, a.Id);
        await app.Complete(process, b.Id);

        var completedSteps = (await app.Send(new GetProcessStatusQuery(process)))!.StepHistory.Take(2).ToArray();

        // Act
        var cancelled = await app.Send(new CancelProcessCommand(process));
        var midway = await app.Send(new GetProcessStatusQuery(process));
        var undoSecond = await app.Complete(process, undoB.Id);
        var undoFirst = await app.Complete(process, undoA.Id);
        var duplicateCancel = await app.Send(new CancelProcessCommand(process));
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.False(cancelled.IsError);
        Assert.Equal(undoB.Id, midway!.StepHistory.Last().StepDefinitionId);
        Assert.DoesNotContain(midway.StepHistory, x => x.StepDefinitionId == undoA.Id);
        Assert.False(undoSecond.IsError);
        Assert.False(undoFirst.IsError);
        Assert.Equal("Process.AlreadyCancelled", duplicateCancel.FirstError.Code);
        Assert.Equal("Cancelled", status!.Status);
        Assert.Equal([undoB.Id, undoA.Id], status.StepHistory.TakeLast(2).Select(x => x.StepDefinitionId));
        Assert.Equal(
            2,
            await app.WithDb(db => db.SagaCompensationLogs.CountAsync(x => x.Status == CompensationStatus.Done)));
        Assert.Equal(
            completedSteps.Select(step => step with { Status = "Compensated" }),
            status.StepHistory.Take(2));
    }

    [Fact]
    public async Task DefinitionVersionConflictAndGlobalStepIdCollision_ReturnDomainErrors()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var command = TestWorkflow.Register(call);
        await app.Send(command);
        // Act
        var version = await app.Send(command);
        var collision = await app.Send(new RegisterProcessDefinitionCommand("different", command.Version, [call]));
        // Assert
        Assert.Equal("ProcessDefinition.VersionConflict", version.FirstError.Code);
        Assert.Equal("ProcessDefinition.StepIdConflict", collision.FirstError.Code);
    }
}
