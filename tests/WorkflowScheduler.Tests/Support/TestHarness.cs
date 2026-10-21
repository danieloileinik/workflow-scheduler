using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Infrastructure.Messaging;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;

namespace WorkflowScheduler.Tests.Support;

internal sealed class TestHarness : IAsyncDisposable
{
    public TestHarness(TestInfrastructure infrastructure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWorkflowScheduler(infrastructure.Configuration(infrastructure.IntegrationPostgres, 0));
        // The integration harness does not start a host. Dispatch/retry cycles are explicit,
        // and the decorator injects publish failures while successful sends still reach Kafka.
        services.RemoveAll<IMessagePublisher>();
        services.AddSingleton<KafkaPublisher>();
        services.AddSingleton<RecordingPublisher>(provider =>
            new RecordingPublisher(provider.GetRequiredService<KafkaPublisher>()));
        services.AddSingleton<IMessagePublisher>(provider => provider.GetRequiredService<RecordingPublisher>());
        Services = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }

    public ServiceProvider Services { get; }
    public RecordingPublisher Publisher => Services.GetRequiredService<RecordingPublisher>();

    public ValueTask DisposeAsync()
    {
        return Services.DisposeAsync();
    }

    public async Task ResetAsync()
    {
        await TestInfrastructure.ResetAsync(Services);
        Publisher.Reset();
    }

    public Task ClearCacheAsync()
    {
        var redis = Services.GetRequiredService<IConnectionMultiplexer>();
        return redis.GetServer(redis.GetEndPoints().Single()).FlushDatabaseAsync(redis.GetDatabase().Database);
    }

    public async Task<T> Send<T>(IRequest<T> command, CancellationToken ct = default)
    {
        await using var scope = Services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<IMediator>().Send(command, ct);
    }

    public async Task<T> WithDb<T>(Func<AppDbContext, Task<T>> action)
    {
        await using var scope = Services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<AppDbContext>());
    }

    public async Task<Guid> Register(params StepDefinitionDto[] steps)
    {
        var result = await Send(TestWorkflow.Register(steps));
        Assert.False(result.IsError, result.IsError ? result.FirstError.Description : "");
        return result.Value.DefinitionId;
    }

    public async Task<Guid> Start(Guid id, Guid? key = null)
    {
        var result = await Send(new StartProcessCommand(id, key ?? Guid.NewGuid(), "order-42", "{}"));
        Assert.False(result.IsError);
        return result.Value.ProcessInstanceId;
    }

    public async Task<ErrorOr<Success>> Complete(Guid processId,
        Guid stepId,
        bool success = true,
        int attempt = 0,
        string? correlation = null)
    {
        var status = await Send(new GetProcessStatusQuery(processId));
        var step = status!.StepHistory.Last(x => x.StepDefinitionId == stepId);
        return await Send(
            new CompleteStepCommand(
                processId,
                stepId,
                success,
                "{}",
                correlation ?? Guid.NewGuid().ToString(),
                step.Id,
                attempt));
    }

    public async Task<int> Dispatch(CancellationToken ct = default)
    {
        await using var scope = Services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<OutboxDispatcher>().DispatchAsync(ct);
    }
}