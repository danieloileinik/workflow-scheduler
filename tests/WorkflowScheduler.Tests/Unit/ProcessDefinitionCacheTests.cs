using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using StackExchange.Redis;
using WorkflowScheduler.Infrastructure.Caching;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ProcessDefinitionCacheTests
{
    [Fact]
    public async Task GetAsync_WhenRedisUnavailable_ReturnsCacheMiss()
    {
        // Arrange
        var (cache, database) = CreateCache();
        database
            .StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(
                Task.FromException<RedisValue>(
                    new RedisConnectionException(
                        ConnectionFailureType.UnableToConnect,
                        "offline")));

        // Act
        var result = await cache.GetAsync(Guid.NewGuid());

        // Assert
        Assert.Null(result);
        Assert.Single(
            database.ReceivedCalls(),
            call =>
                call.GetMethodInfo().Name == nameof(IDatabase.StringGetAsync));
    }

    [Fact]
    public async Task SetAsync_WhenRedisUnavailable_DoesNotThrow()
    {
        // Arrange
        var (cache, database) = CreateCache();
        database
            .StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<bool>(),
                Arg.Any<When>(),
                Arg.Any<CommandFlags>())
            .Returns(
                Task.FromException<bool>(
                    new RedisConnectionException(
                        ConnectionFailureType.UnableToConnect,
                        "offline")));
        var definition = new ProcessDefinition("workflow", 1);

        // Act
        await cache.SetAsync(definition);

        // Assert
        Assert.Single(
            database.ReceivedCalls(),
            call =>
                call.GetMethodInfo().Name == nameof(IDatabase.StringSetAsync));
    }

    [Theory]
    [InlineData("invalid-json")]
    [InlineData("null")]
    [InlineData("{}")]
    public async Task GetAsync_WithCorruptSnapshot_ReturnsCacheMiss(string snapshot)
    {
        // Arrange
        var (cache, database) = CreateCache();
        RedisValue value = snapshot;
        database.StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>()).Returns(value);

        // Act
        var result = await cache.GetAsync(Guid.NewGuid());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAsync_AfterSnapshotStored_PreservesDefinitionMetadataAndStepOrder()
    {
        // Arrange
        var (cache, database) = CreateCache();
        var definition = new ProcessDefinition("workflow", 7)
        {
            CreatedAt = DateTime.UtcNow.AddDays(-1)
        };
        const string callMetadata = """{"serviceName":"billing","command":"Execute"}""";
        const string waitMetadata = """{"eventName":"Approved"}""";
        var first = new StepDefinition(definition.Id, "first", StepType.Call, callMetadata);
        var second = new StepDefinition(definition.Id, "second", StepType.WaitEvent, waitMetadata);
        definition.AddStep(first);
        definition.AddStep(second);
        var savedSnapshot = RedisValue.Null;
        database
            .StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Do<RedisValue>(value => savedSnapshot = value),
                Arg.Any<TimeSpan?>(),
                Arg.Any<bool>(),
                Arg.Any<When>(),
                Arg.Any<CommandFlags>())
            .Returns(true);
        database
            .StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(_ => savedSnapshot);
        await cache.SetAsync(definition);

        // Act
        var result = await cache.GetAsync(definition.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(definition.Id, result.Id);
        Assert.Equal(definition.Name, result.Name);
        Assert.Equal(definition.CreatedAt, result.CreatedAt);
        Assert.Equal(definition.Version, result.Version);
        Assert.Equal([first.Id, second.Id], result.Steps.Select(step => step.Id));
        Assert.Equal([callMetadata, waitMetadata], result.Steps.Select(step => step.Metadata));
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("[null]")]
    [InlineData("[{}]")]
    public async Task GetAsync_WithInvalidStepSnapshots_ReturnsCacheMiss(string steps)
    {
        var (cache, database) = CreateCache();
        var id = Guid.NewGuid();
        RedisValue snapshot = $$"""{"Id":"{{id}}","Name":"workflow","Version":1,"Steps":{{steps}}} """;
        database.StringGetAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>()).Returns(snapshot);

        Assert.Null(await cache.GetAsync(id));
    }

    private static (ProcessDefinitionCache Cache, IDatabase Database) CreateCache()
    {
        var redis = Substitute.For<IConnectionMultiplexer>();
        var database = Substitute.For<IDatabase>();
        redis.GetDatabase().Returns(database);
        var cache = new ProcessDefinitionCache(redis, NullLogger<ProcessDefinitionCache>.Instance);
        return (cache, database);
    }
}