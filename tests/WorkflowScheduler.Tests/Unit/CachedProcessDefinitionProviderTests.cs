using NSubstitute;
using WorkflowScheduler.Features.Processes;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class CachedProcessDefinitionProviderTests
{
    [Fact]
    public async Task GetOrLoadAsync_WhenCached_ReturnsDefinitionWithoutDatabaseAccessOrCacheWrite()
    {
        // Arrange
        var definition = new ProcessDefinition("workflow", 1);
        var cache = Substitute.For<IProcessDefinitionCache>();
        using var cancellation = new CancellationTokenSource();
        cache.GetAsync(definition.Id, cancellation.Token).Returns(Task.FromResult<ProcessDefinition?>(definition));
        await using var db = new AppDbContext(new DbContextOptionsBuilder<AppDbContext>().Options);
        var provider = new CachedProcessDefinitionProvider(db, cache);

        // Act
        var result = await provider.GetOrLoadAsync(definition.Id, cancellation.Token);

        // Assert
        Assert.Same(definition, result);
        await cache.Received(1).GetAsync(definition.Id, cancellation.Token);
        await cache.DidNotReceive().SetAsync(Arg.Any<ProcessDefinition>(), Arg.Any<CancellationToken>());
    }
}