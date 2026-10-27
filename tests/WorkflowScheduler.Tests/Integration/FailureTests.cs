using FluentValidation;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class FailureTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task DuplicateCompletion_DoesNotAdvanceOrPublishTwice()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var second = TestWorkflow.Call();
        var first = TestWorkflow.Call(second.Id);
        var process = await app.Start(await app.Register(first, second));
        var status = await app.Send(new GetProcessStatusQuery(process));
        var callback = new CompleteStepCommand(
            process,
            first.Id,
            true,
            "{}",
            "callback-1",
            status!.StepHistory.Single().Id);
        // Act
        var success = await app.Send(callback);
        var duplicate = await app.Send(callback);
        // Assert
        Assert.False(success.IsError);
        Assert.Equal("Process.DuplicateEvent", duplicate.FirstError.Code);
        Assert.Equal(2, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Equal(1, await app.WithDb(db => db.EventLogs.CountAsync()));
    }

    [Fact]
    public async Task FailureAfterRetries_QueuesActualCompensationDefinition()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var undo = TestWorkflow.Call(service: "refund");
        var failing = TestWorkflow.Call(service: "shipping");
        var first = TestWorkflow.Call(failing.Id, undo.Id);
        var process = await app.Start(await app.Register(first, failing, undo));
        await app.Complete(process, first.Id);
        await app.WithDb(async db =>
        {
            var step = await db.StepInstances.SingleAsync(x =>
                x.ProcessInstanceId == process && x.StepDefinitionId == failing.Id);
            db.Entry(step).Property(x => x.RetryCount).CurrentValue = step.MaxRetries;

            return await db.SaveChangesAsync();
        });
        // Act
        var failed = await app.Complete(process, failing.Id, false, 3);
        var compensated = await app.Complete(process, undo.Id);
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.False(failed.IsError);
        Assert.False(compensated.IsError);
        Assert.Equal("Failed", status!.Status);
        Assert.Equal("Compensated", status.StepHistory.First().Status);
        Assert.Equal(
            undo.Id,
            await app.WithDb(db => db.SagaCompensationLogs.Select(x => x.StepDefinitionId).SingleAsync()));
    }

    [Fact]
    public async Task CompensationExhaustion_BlocksOlderCompensationAndRecordsFailure()
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
        await app.Send(new CancelProcessCommand(process));
        await app.WithDb(async db =>
        {
            var step = await db.StepInstances.SingleAsync(x => x.ProcessInstanceId == process && x.IsCompensation);
            db.Entry(step).Property(x => x.RetryCount).CurrentValue = step.MaxRetries;

            return await db.SaveChangesAsync();
        });
        // Act
        var failed = await app.Complete(process, undoB.Id, false, 3);
        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.False(failed.IsError);
        Assert.DoesNotContain(status!.StepHistory, x => x.StepDefinitionId == undoA.Id);
        Assert.Equal(
            1,
            await app.WithDb(db => db.SagaCompensationLogs.CountAsync(x => x.Status == CompensationStatus.Failed)));
        Assert.Equal(
            1,
            await app.WithDb(db => db.SagaCompensationLogs.CountAsync(x => x.Status == CompensationStatus.Pending)));
    }

    [Fact]
    public async Task CancelledPendingRetry_IsNeverDispatched()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        await app.Complete(process, call.Id, false);
        // Act
        await app.Send(new CancelProcessCommand(process));
        await app.Send(new RetryStepsCommand(process));

        var status = await app.Send(new GetProcessStatusQuery(process));
        // Assert
        Assert.Equal("Cancelled", status!.StepHistory.Single().Status);
        Assert.Equal(1, await app.WithDb(db => db.OutboxMessages.CountAsync()));
        Assert.Equal(0, await app.WithDb(db => db.StepInstances.CountAsync(x => x.NextRetryAt != null)));
    }

    [Theory]
    [InlineData("Complete")]
    [InlineData("Cancel")]
    [InlineData("Retry")]
    [InlineData("Event")]
    public async Task MissingProcess_ReturnsDomainError(string operation)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var process = Guid.NewGuid();
        IRequest<ErrorOr<Success>> request = operation switch
        {
            "Complete" => new CompleteStepCommand(process, Guid.NewGuid(), true, null, "missing", Guid.NewGuid()),
            "Cancel" => new CancelProcessCommand(process),
            "Retry" => new RetryStepsCommand(process),
            _ => new SendEventCommand(process, "Approved", null, "missing")
        };
        // Act
        var result = await app.Send(request);
        // Assert
        Assert.Equal("Process.NotFound", result.FirstError.Code);
    }

    [Fact]
    public async Task InvalidCommand_IsRejectedBeforeAnyDatabaseWrite()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var command = new StartProcessCommand(Guid.NewGuid(), Guid.Empty, "order", "{}");
        // Act
        await Assert.ThrowsAsync<ValidationException>(() => app.Send(command));
        // Assert
        Assert.Equal(0, await app.WithDb(db => db.StartRequests.CountAsync()));
        Assert.Equal(0, await app.WithDb(db => db.ProcessInstances.CountAsync()));
    }
}