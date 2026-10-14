using StackExchange.Redis;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class CacheTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task Start_PopulatesRealRedisCacheWithOrderedDefinitionAndTtl()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var second = TestWorkflow.Wait();
        var first = TestWorkflow.Call(second.Id);
        var definition = await app.Register(first, second);

        // Act
        await app.Start(definition);
        await using var scope = app.Services.CreateAsyncScope();
        var cached = await scope.ServiceProvider.GetRequiredService<IProcessDefinitionCache>().GetAsync(definition);
        var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var ttl = await redis.KeyTimeToLiveAsync($"proc-def:v2:{definition}");

        // Assert
        Assert.NotNull(cached);
        Assert.Equal(first.Id, cached.GetFirstStep().Value.Id);
        Assert.Equal([first.Id, second.Id], cached.Steps.Select(step => step.Id));
        Assert.Equal([first.Metadata, second.Metadata], cached.Steps.Select(step => step.Metadata));
        Assert.NotNull(ttl);
        Assert.InRange(ttl.Value, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(30));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MissingOrCorruptSnapshot_FallsBackToPostgresAndRefillsRedis(bool corrupt)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var definition = await app.Register(call);
        await app.Start(definition);
        var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var key = $"proc-def:v2:{definition}";
        if (corrupt) await redis.StringSetAsync(key, "{broken-json");
        else await redis.KeyDeleteAsync(key);

        // Act
        var process = await app.Start(definition);
        await using var scope = app.Services.CreateAsyncScope();
        var cached = await scope.ServiceProvider.GetRequiredService<IProcessDefinitionCache>().GetAsync(definition);

        // Assert
        Assert.NotNull(cached);
        Assert.Equal(call.Id, cached.GetFirstStep().Value.Id);
        Assert.Equal(2, await app.WithDb(db => db.ProcessInstances.CountAsync()));
        Assert.Equal(2, await app.WithDb(db => db.StartRequests.CountAsync()));
        var status = await app.Send(new GetProcessStatusQuery(process));
        Assert.Equal(call.Id, status!.StepHistory.Single().StepDefinitionId);
        Assert.Equal("Running", status.StepHistory.Single().Status);
    }
}