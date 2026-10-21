using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Infrastructure.Caching;
using WorkflowScheduler.Infrastructure.Messaging;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class WorkflowInfrastructureConfigurationTests
{
    [Fact]
    public async Task AddWorkflowScheduler_WithoutRedis_ResolvesDefinitionProviderAndUsesCacheMisses()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Postgres"] =
                        "Host=localhost;Database=workflow;Username=workflow;Password=workflow",
                    ["Kafka:BootstrapServers"] = "localhost:9092"
                })
            .Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddWorkflowScheduler(configuration);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var definition = new ProcessDefinition("workflow", 1);

        // Act
        var definitions = scope.ServiceProvider.GetRequiredService<CachedProcessDefinitionProvider>();
        var cache = scope.ServiceProvider.GetRequiredService<IProcessDefinitionCache>();
        await cache.SetAsync(definition);
        var cached = await cache.GetAsync(definition.Id);

        // Assert
        Assert.NotNull(definitions);
        Assert.IsType<DisabledProcessDefinitionCache>(cache);
        Assert.Null(cached);
        Assert.DoesNotContain(services, service => service.ServiceType == typeof(IConnectionMultiplexer));
    }

    [Fact]
    public void AddWorkflowScheduler_WithRedis_RegistersRedisCache()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Redis"] = "cache:6379"
                })
            .Build();
        var services = new ServiceCollection();

        // Act
        services.AddWorkflowScheduler(configuration);

        // Assert
        Assert.Contains(services, service => service.ServiceType == typeof(IConnectionMultiplexer));
        Assert.Contains(
            services,
            service => service.ServiceType == typeof(IProcessDefinitionCache)
                       && service.ImplementationType == typeof(ProcessDefinitionCache));
    }

    [Fact]
    public void KafkaConfiguration_WithSaslTls_PreservesSecurityAndEnforcesDeliveryGuarantees()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["Kafka:BootstrapServers"] = "broker:9093",
                    ["Kafka:ProducerConfig:SecurityProtocol"] = "SaslSsl",
                    ["Kafka:ProducerConfig:SaslMechanism"] = "ScramSha512",
                    ["Kafka:ProducerConfig:SaslUsername"] = "scheduler",
                    ["Kafka:ProducerConfig:SaslPassword"] = "test-password",
                    ["Kafka:ProducerConfig:SslCaLocation"] = "/certificates/kafka-ca.pem",
                    ["Kafka:ProducerConfig:EnableIdempotence"] = "false",
                    ["Kafka:ProducerConfig:Acks"] = "Leader"
                })
            .Build();
        var services = new ServiceCollection();
        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;

        // Act
        var producer = options.CreateProducerConfig();

        // Assert
        Assert.Equal("broker:9093", producer.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslSsl, producer.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, producer.SaslMechanism);
        Assert.Equal("scheduler", producer.SaslUsername);
        Assert.Equal("test-password", producer.SaslPassword);
        Assert.Equal("/certificates/kafka-ca.pem", producer.SslCaLocation);
        Assert.True(producer.EnableIdempotence);
        Assert.Equal(Acks.All, producer.Acks);
        Assert.False(options.ProducerConfig.EnableIdempotence);
        Assert.Equal(Acks.Leader, options.ProducerConfig.Acks);
    }
}