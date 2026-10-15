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
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedProcess_IsDeferredAndDoesNotPreventOtherDueRetries(bool throws)
    {
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var definition = await app.Register(call);
        var bad = await app.Start(definition);
        var good = await app.Start(definition);
        await app.Complete(bad, call.Id, false);
        await app.Complete(good, call.Id, false);
        await app.WithDb(db => db.StepInstances.ExecuteUpdateAsync(update =>
            update.SetProperty(step => step.NextRetryAt, DateTime.UtcNow.AddMinutes(-1))));
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWorkflowScheduler(infrastructure.Configuration(infrastructure.IntegrationPostgres, 0));
        services.AddScoped<IMediator>(provider =>
        {
            var inner = new Mediator(provider);
            var mediator = Substitute.For<IMediator>();
            mediator.Send(Arg.Any<RetryStepsCommand>(), Arg.Any<CancellationToken>()).Returns(callInfo =>
            {
                var command = callInfo.Arg<RetryStepsCommand>();
                if (command.ProcessInstanceId != bad) return inner.Send(command, callInfo.Arg<CancellationToken>());
                return throws
                    ? Task.FromException<ErrorOr<Success>>(new InvalidOperationException("Injected retry failure"))
                    : Task.FromResult<ErrorOr<Success>>(Error.Failure("Retry.Failed", "Injected rejection"));
            });
            return mediator;
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var worker = new RetryWorker(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<RetryWorker>.Instance);

        Assert.Equal(2, await worker.DispatchAsync(CancellationToken.None));

        var rejected = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync(step => step.ProcessInstanceId == bad));
        var dispatched = await app.WithDb(db => db.StepInstances.AsNoTracking().SingleAsync(step => step.ProcessInstanceId == good));
        Assert.Equal(StepStatus.Pending, rejected.Status);
        Assert.True(rejected.NextRetryAt > DateTime.UtcNow);
        Assert.Equal(StepStatus.Running, dispatched.Status);
        Assert.Equal(1, rejected.RetryCount);
        Assert.Equal(1, dispatched.RetryCount);
        Assert.Equal(0, await worker.DispatchAsync(CancellationToken.None));
    }
}
