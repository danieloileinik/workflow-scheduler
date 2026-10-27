using StackExchange.Redis;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class IsolationTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task ReusedInfrastructure_ResetsDatabaseCacheAndKafkaConsumerOffsets()
    {
        // Arrange: leave persisted rows, a cache entry and a broker record from an earlier test.
        var previous = await infrastructure.CreateHarnessAsync();
        var oldDefinition = await previous.Register(TestWorkflow.Call());
        var oldProcess = await previous.Start(oldDefinition);
        await previous.Dispatch();

        var redis = previous.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        Assert.True(await redis.KeyExistsAsync($"proc-def:v2:{oldDefinition}"));

        // Act: reuse the fixture and the provider, clearing only their test data.
        var current = await infrastructure.CreateHarnessAsync();
        using var broker = infrastructure.CreateProbe("billing");
        var definition = await current.Register(TestWorkflow.Call());
        var process = await current.Start(definition);
        await current.Dispatch();

        var delivered = await broker.ReadAsync("billing", process);

        // Assert
        Assert.Same(previous, current);
        Assert.Same(previous.Services, current.Services);
        Assert.False(await redis.KeyExistsAsync($"proc-def:v2:{oldDefinition}"));
        Assert.Equal(1, await current.WithDb(db => db.ProcessDefinitions.CountAsync()));
        Assert.Equal(1, await current.WithDb(db => db.ProcessInstances.CountAsync()));
        Assert.False(await current.WithDb(db => db.ProcessInstances.AnyAsync(instance => instance.Id == oldProcess)));
        Assert.Equal(process, delivered.ProcessInstanceId);
        Assert.Single(current.Publisher.Messages);
        Assert.Equal(0, broker.CountRemaining(process));
    }
}