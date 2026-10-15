using System.Text.Json;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
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
        Assert.Equal(0, await app.Dispatch());
        await app.WithDb(db => db.OutboxMessages.ExecuteUpdateAsync(update =>
            update.SetProperty(message => message.NextAttemptAt, DateTime.UtcNow.AddSeconds(-1))));
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

        // Act: the durable claim excludes competitors without holding row locks during publication.
        try
        {
            await publishing.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await app.WithDb(async db =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync(timeout.Token);
                var messages = await db
                    .OutboxMessages
                    .FromSqlRaw(
                        "SELECT * FROM outbox_messages FOR UPDATE NOWAIT")
                    .AsNoTracking()
                    .ToListAsync(timeout.Token);
                Assert.Equal(4, messages.Count);
                return 0;
            });
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

    [Fact]
    public async Task PoisonBatch_IsQuarantinedAndDoesNotStarveFollowingValidMessage()
    {
        var app = await infrastructure.CreateHarnessAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var process = await app.Start(await app.Register(TestWorkflow.Call()));
        await app.WithDb(async db =>
        {
            db.OutboxMessages.AddRange(
                Enumerable
                    .Range(0, 100)
                    .Select(_ => new OutboxMessage
                    {
                        Type = "process.commands.billing", Payload = "{invalid-json",
                        CreatedAt = DateTime.UtcNow.AddDays(-1)
                    }));
            return await db.SaveChangesAsync();
        });

        Assert.Equal(100, await app.Dispatch());
        Assert.Equal(1, await app.Dispatch());

        Assert.Equal(process, (await broker.ReadAsync("billing", process)).ProcessInstanceId);
        Assert.Equal(100, await app.WithDb(db => db.OutboxMessages.CountAsync(message => message.FailedAt != null)));
        Assert.Single(app.Publisher.Messages);
    }

    [Fact]
    public async Task RepeatedPublishFailures_BackOffAndQuarantineAfterBoundedAttempts()
    {
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        app.Publisher.Fail = true;
        for (var attempt = 1; attempt <= 8; attempt++)
        {
            Assert.Equal(1, await app.Dispatch());
            var message = await app.WithDb(db => db.OutboxMessages.AsNoTracking().SingleAsync());
            Assert.Equal(attempt, message.Attempts);
            Assert.Null(message.ProcessedAt);
            if (attempt < 8)
            {
                Assert.NotNull(message.NextAttemptAt);
                Assert.Null(message.FailedAt);
                Assert.Equal(0, await app.Dispatch());
                await app.WithDb(db => db.OutboxMessages.ExecuteUpdateAsync(update =>
                    update.SetProperty(candidate => candidate.NextAttemptAt, DateTime.UtcNow.AddSeconds(-1))));
            }
            else
            {
                Assert.NotNull(message.FailedAt);
            }
        }

        app.Publisher.Fail = false;
        Assert.Equal(0, await app.Dispatch());
        Assert.Empty(app.Publisher.Messages);
    }

    [Fact]
    public async Task ExpiredClaim_IsRecoveredAfterAnInterruptedDispatcher()
    {
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        await app.WithDb(db => db.OutboxMessages.ExecuteUpdateAsync(update =>
            update
                .SetProperty(message => message.ClaimId, Guid.NewGuid())
                .SetProperty(message => message.ClaimedUntil, DateTime.UtcNow.AddMinutes(1))));
        Assert.Equal(0, await app.Dispatch());
        await app.WithDb(db => db.OutboxMessages.ExecuteUpdateAsync(update =>
            update.SetProperty(message => message.ClaimedUntil, DateTime.UtcNow.AddSeconds(-1))));

        Assert.Equal(1, await app.Dispatch());

        var delivered = await app.WithDb(db => db.OutboxMessages.AsNoTracking().SingleAsync());
        Assert.NotNull(delivered.ProcessedAt);
        Assert.Null(delivered.ClaimId);
        Assert.Null(delivered.ClaimedUntil);
        Assert.Single(app.Publisher.Messages);
    }

    [Fact]
    public async Task DatabaseUpgrade_PreservesExistingOutboxAndCanRunAgain()
    {
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        await app.WithDb(async db =>
        {
            var original = await db.OutboxMessages.AsNoTracking().SingleAsync();
            await db.Database.ExecuteSqlRawAsync(
                """
                ALTER TABLE outbox_messages DROP COLUMN "Attempts", DROP COLUMN "NextAttemptAt",
                    DROP COLUMN "FailedAt", DROP COLUMN "ClaimId", DROP COLUMN "ClaimedUntil"
                """);

            await DatabaseInitializer.InitializeAsync(db);
            await DatabaseInitializer.InitializeAsync(db);

            var upgraded = await db.OutboxMessages.AsNoTracking().SingleAsync();
            Assert.Equal(original.Id, upgraded.Id);
            Assert.Equal(original.Payload, upgraded.Payload);
            Assert.Equal(0, upgraded.Attempts);
            Assert.Null(upgraded.NextAttemptAt);
            Assert.Null(upgraded.FailedAt);
            Assert.Null(upgraded.ClaimId);
            return 0;
        });
        Assert.Equal(1, await app.Dispatch());
    }

    [Fact]
    public async Task Cancellation_ReleasesUnfinishedClaimsForTheNextDispatcher()
    {
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        using var cancellation = new CancellationTokenSource();
        app.Publisher.BeforePublishAsync = _ =>
        {
            cancellation.Cancel();
            return Task.FromCanceled(cancellation.Token);
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => app.Dispatch(cancellation.Token));

        var pending = await app.WithDb(db => db.OutboxMessages.AsNoTracking().SingleAsync());
        Assert.Null(pending.ProcessedAt);
        Assert.Null(pending.ClaimId);
        Assert.Null(pending.ClaimedUntil);
        app.Publisher.BeforePublishAsync = null;
        Assert.Equal(1, await app.Dispatch());
    }

    [Fact]
    public async Task DispatcherThatLostItsClaim_CannotAcknowledgeOrReleaseANewOwnersMessage()
    {
        var app = await infrastructure.CreateHarnessAsync();
        await app.Start(await app.Register(TestWorkflow.Call()));
        var newOwner = Guid.NewGuid();
        app.Publisher.BeforePublishAsync = _ => app.WithDb(db => db.OutboxMessages.ExecuteUpdateAsync(update =>
            update
                .SetProperty(message => message.ClaimId, newOwner)
                .SetProperty(message => message.ClaimedUntil, DateTime.UtcNow.AddMinutes(1))));

        await app.Dispatch();

        var pending = await app.WithDb(db => db.OutboxMessages.AsNoTracking().SingleAsync());
        Assert.Equal(newOwner, pending.ClaimId);
        Assert.Null(pending.ProcessedAt);
        Assert.Empty(
            await app.WithDb(db => db.OutboxMessages.Where(message => message.FailedAt != null).ToListAsync()));
    }
}