using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class RetryWorkerTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task FailedProcess_IsDeferredAndDoesNotPreventOtherDueRetries(bool throws, bool recovery)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var definition = await app.Register(call);
        var bad = await app.Start(definition);
        var good = await app.Start(definition);
        if (recovery)
        {
            await app.Dispatch();
        }
        else
        {
            await app.Complete(bad, call.Id, false);
            await app.Complete(good, call.Id, false);
        }

        var due = DateTime.UtcNow.AddMinutes(-1);
        await app.WithDb(db => db.StepInstances.ExecuteUpdateAsync(update =>
            update
                .SetProperty(step => step.NextRetryAt, recovery ? (DateTime?)null : due)
                .SetProperty(step => step.NextRecoveryAt, recovery ? due : (DateTime?)null)));

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWorkflowScheduler(infrastructure.Configuration(infrastructure.IntegrationPostgres, 0));
        services.AddScoped<IMediator>(provider =>
        {
            var inner = new Mediator(provider);
            var mediator = Substitute.For<IMediator>();
            mediator
                .Send(Arg.Any<RetryStepsCommand>(), Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    var command = callInfo.Arg<RetryStepsCommand>();
                    if (command.ProcessInstanceId != bad)
                        return inner.Send(command, callInfo.Arg<CancellationToken>());

                    return throws
                        ? Task.FromException<ErrorOr<Success>>(new InvalidOperationException("Injected retry failure"))
                        : Task.FromResult<ErrorOr<Success>>(Error.Failure("Retry.Failed", "Injected rejection"));
                });

            return mediator;
        });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var worker = new RetryWorker(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<RetryWorker>.Instance);

        // Act
        var count = await worker.DispatchAsync(CancellationToken.None);

        var rejected = await app.WithDb(db =>
            db.StepInstances.AsNoTracking().SingleAsync(step => step.ProcessInstanceId == bad));
        var dispatched = await app.WithDb(db =>
            db.StepInstances.AsNoTracking().SingleAsync(step => step.ProcessInstanceId == good));
        var subsequent = await worker.DispatchAsync(CancellationToken.None);

        // Assert
        Assert.Equal(2, count);
        Assert.Equal(recovery ? StepStatus.Running : StepStatus.Pending, rejected.Status);
        Assert.True((recovery ? rejected.NextRecoveryAt : rejected.NextRetryAt) > DateTime.UtcNow);
        Assert.Equal(StepStatus.Running, dispatched.Status);
        Assert.Null(dispatched.NextRecoveryAt);
        Assert.Equal(recovery ? 0 : 1, rejected.RetryCount);
        Assert.Equal(recovery ? 0 : 1, dispatched.RetryCount);
        Assert.Equal(0, subsequent);
    }
}
