using NSubstitute;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Infrastructure.Caching;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class CachedProcessDefinitionProviderTests
{
    [Fact]
    public async Task GetOrLoadAsync_ReusesImmutableDefinitionAcrossConcurrentRequests()
    {
        // Arrange
        var definition = new ProcessDefinition("workflow", 1);
        definition.AddStep(
            new StepDefinition(definition.Id, "wait", StepType.WaitEvent, """{"eventName":"Approved"}"""));

        var cache = Substitute.For<IProcessDefinitionCache>();
        var loaded = new TaskCompletionSource<ProcessDefinition?>(TaskCreationOptions.RunContinuationsAsynchronously);
        cache.GetAsync(definition.Id, Arg.Any<CancellationToken>()).Returns(loaded.Task);

        await using var services = CreateServices(cache);
        await using var scope = services.CreateAsyncScope();
        var provider = scope.ServiceProvider.GetRequiredService<CachedProcessDefinitionProvider>();

        // Act
        Task<PreparedProcessDefinition?>[] requests =
        [
            .. Enumerable.Range(0, 16).Select(_ => provider.GetOrLoadAsync(definition.Id, CancellationToken.None))
        ];
        loaded.SetResult(definition);

        var results = await Task.WhenAll(requests);
        definition.AddStep(new StepDefinition(definition.Id, "later", StepType.WaitEvent, """{"eventName":"Later"}"""));

        // Assert
        Assert.NotNull(results[0]);
        Assert.All(results, result => Assert.Same(results[0], result));
        Assert.Single(results[0]!.Steps);
        Assert.Equal("Approved", Assert.IsType<StepMetadata.WaitEvent>(results[0]!.Steps[0].Metadata).EventName);
        await cache.Received(1).GetAsync(definition.Id, Arg.Any<CancellationToken>());
        await cache.DidNotReceive().SetAsync(Arg.Any<ProcessDefinition>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetOrLoadAsync_WhenDefinitionExceedsCapacity_DoesNotRetainIt()
    {
        // Arrange
        var definition = new ProcessDefinition("workflow", 1);
        definition.AddStep(
            new StepDefinition(definition.Id, "wait", StepType.WaitEvent, """{"eventName":"Approved"}"""));

        var cache = Substitute.For<IProcessDefinitionCache>();
        cache
            .GetAsync(definition.Id, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ProcessDefinition?>(definition));

        await using var services = CreateServices(cache, 1);
        await using var scope = services.CreateAsyncScope();
        var provider = scope.ServiceProvider.GetRequiredService<CachedProcessDefinitionProvider>();

        // Act
        var first = await provider.GetOrLoadAsync(definition.Id, CancellationToken.None);
        var second = await provider.GetOrLoadAsync(definition.Id, CancellationToken.None);

        // Assert
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.NotSame(first, second);
        await cache.Received(2).GetAsync(definition.Id, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetOrLoadAsync_WhenCancelled_DoesNotReadEvenAWarmCache()
    {
        // Arrange
        var definition = new ProcessDefinition("workflow", 1);
        definition.AddStep(
            new StepDefinition(definition.Id, "wait", StepType.WaitEvent, """{"eventName":"Approved"}"""));

        var cache = Substitute.For<IProcessDefinitionCache>();
        cache
            .GetAsync(definition.Id, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<ProcessDefinition?>(definition));

        await using var services = CreateServices(cache);
        await using var scope = services.CreateAsyncScope();
        var provider = scope.ServiceProvider.GetRequiredService<CachedProcessDefinitionProvider>();
        await provider.GetOrLoadAsync(definition.Id, CancellationToken.None);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        // Act
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            provider.GetOrLoadAsync(definition.Id, cancellation.Token));

        // Assert
        await cache.Received(1).GetAsync(definition.Id, Arg.Any<CancellationToken>());
    }

    private static ServiceProvider CreateServices(IProcessDefinitionCache cache, long capacityBytes = 32 * 1024 * 1024)
    {
        var services = new ServiceCollection();
        services.AddSingleton(cache);
        services.AddSingleton(_ => new PreparedProcessDefinitionCache(capacityBytes));
        services.AddScoped(_ => new AppDbContext(new DbContextOptionsBuilder<AppDbContext>().Options));
        services.AddScoped<CachedProcessDefinitionProvider>();

        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }
}