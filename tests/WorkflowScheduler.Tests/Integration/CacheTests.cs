using StackExchange.Redis;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class CacheTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompleteStep_WithMissingOrCorruptSnapshot_RefillsCacheAndAdvancesProcess(bool corrupt)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var second = TestWorkflow.Wait();
        var first = TestWorkflow.Call(second.Id);
        var definition = await app.Register(first, second);
        var process = await app.Start(definition);
        var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        var key = $"proc-def:v2:{definition}";
        if (corrupt) await redis.StringSetAsync(key, "{broken-json");
        else await redis.KeyDeleteAsync(key);

        // Act
        var result = await app.Complete(process, first.Id);

        // Assert
        Assert.False(result.IsError);
        await using var scope = app.Services.CreateAsyncScope();
        var cached = await scope.ServiceProvider.GetRequiredService<IProcessDefinitionCache>().GetAsync(definition);
        Assert.NotNull(cached);
        Assert.Equal([first.Id, second.Id], cached.Steps.Select(step => step.Id));
        Assert.Equal([first.Metadata, second.Metadata], cached.Steps.Select(step => step.Metadata));
        var status = await app.Send(new GetProcessStatusQuery(process));
        Assert.Equal("Waiting", status!.Status);
        Assert.Equal(["Completed", "Waiting"], status.StepHistory.Select(step => step.Status));
    }

    [Fact]
    public async Task GetOrLoadAsync_WhenDefinitionMissing_ReturnsNullWithoutCreatingCacheEntry()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var definitionId = Guid.NewGuid();
        await using var scope = app.Services.CreateAsyncScope();
        var provider = scope.ServiceProvider.GetRequiredService<CachedProcessDefinitionProvider>();
        var redis = app.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();

        // Act
        var result = await provider.GetOrLoadAsync(definitionId, CancellationToken.None);

        // Assert
        Assert.Null(result);
        Assert.False(await redis.KeyExistsAsync($"proc-def:v2:{definitionId}"));
    }

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