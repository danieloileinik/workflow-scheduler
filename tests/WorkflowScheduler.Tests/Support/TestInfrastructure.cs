using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using Npgsql;
using StackExchange.Redis;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using WorkflowScheduler.Infrastructure.Caching;

namespace WorkflowScheduler.Tests.Support;

// xUnit creates one fixture for the entire collection, including filtered runs.
// Containers, schemas, connection pools and the integration service provider are reused.
public sealed class TestInfrastructure : IAsyncLifetime
{
    private bool _disposed;
    private TestHarness? _harness;
    private KafkaContainer? _kafka;
    private PostgreSqlContainer? _postgres;
    private RedisContainer? _redis;

    internal string IntegrationPostgres { get; private set; } = null!;
    internal string EndToEndPostgres { get; private set; } = null!;
    internal string BootstrapServers => _kafka!.GetBootstrapAddress();

    public async Task InitializeAsync()
    {
        try
        {
            _postgres = new PostgreSqlBuilder("postgres:18.6-alpine3.24").WithDatabase("workflow_integration").Build();
            _redis = new RedisBuilder("redis:8.10.2-alpine3.23").Build();
            _kafka = new KafkaBuilder("apache/kafka:4.3.2")
                .WithKRaft()
                .Build();

            using var startupTimeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
            await Task.WhenAll(
                _postgres.StartAsync(startupTimeout.Token),
                _redis.StartAsync(startupTimeout.Token),
                _kafka.StartAsync(startupTimeout.Token));

            IntegrationPostgres = _postgres.GetConnectionString();
            EndToEndPostgres = new NpgsqlConnectionStringBuilder(IntegrationPostgres)
                { Database = "workflow_e2e" }.ConnectionString;

            await using (var connection = new NpgsqlConnection(IntegrationPostgres))
            {
                await connection.OpenAsync(startupTimeout.Token);

                await using var command = new NpgsqlCommand("CREATE DATABASE workflow_e2e", connection);
                await command.ExecuteNonQueryAsync(startupTimeout.Token);
            }

            await Task.WhenAll(
                CreateSchemaAsync(IntegrationPostgres, startupTimeout.Token),
                CreateSchemaAsync(EndToEndPostgres, startupTimeout.Token),
                CreateTopicsAsync().WaitAsync(startupTimeout.Token));
            _harness = new TestHarness(this);
        }
        catch (Exception exception)
        {
            var failure = exception;

            try
            {
                await DisposeAsync();
            }
            catch (Exception cleanupException)
            {
                failure = new AggregateException(
                    "Infrastructure startup and cleanup both failed.",
                    exception,
                    cleanupException);
            }

            throw new InvalidOperationException(
                "Could not start the PostgreSQL/Redis/Kafka test infrastructure. " +
                "Docker must be running and accessible to the test process. " +
                "For a Docker-free run use --filter 'Category=Unit'.",
                failure);
        }
    }

    public async Task DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            if (_harness is not null)
                await _harness.DisposeAsync();
        }
        finally
        {
            // Always attempt to dispose every container, including partial startup failures.
            await Task.WhenAll(
                _kafka?.DisposeAsync().AsTask() ?? Task.CompletedTask,
                _redis?.DisposeAsync().AsTask() ?? Task.CompletedTask,
                _postgres?.DisposeAsync().AsTask() ?? Task.CompletedTask);
        }
    }

    internal IConfiguration Configuration(string postgres, int redisDatabase)
    {
        var redis = ConfigurationOptions.Parse(_redis!.GetConnectionString());
        redis.DefaultDatabase = redisDatabase;
        redis.AllowAdmin = true;

        return new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Postgres"] = postgres,
                    ["ConnectionStrings:Redis"] = redis.ToString(),
                    ["Kafka:BootstrapServers"] = BootstrapServers,
                    ["Logging:LogLevel:Default"] = "Warning"
                })
            .Build();
    }

    internal async Task<TestHarness> CreateHarnessAsync()
    {
        await _harness!.ResetAsync();

        return _harness;
    }

    internal async Task<GrpcApp> CreateGrpcAppAsync()
    {
        // Each test disposes its lightweight host BEFORE the next reset. This prevents
        // in-flight outbox/retry workers from racing TRUNCATE or contaminating the next test.
        var options = new DbContextOptionsBuilder<AppDbContext>().UseNpgsql(EndToEndPostgres).Options;
        await using var db = new AppDbContext(options);
        await ResetDatabaseAsync(db);

        var redis = _harness!.Services.GetRequiredService<IConnectionMultiplexer>();
        await redis.GetServer(redis.GetEndPoints().Single()).FlushDatabaseAsync(1);

        return new GrpcApp(this);
    }

    internal KafkaProbe CreateProbe(params string[] services)
    {
        return new KafkaProbe(BootstrapServers, services);
    }

    internal static async Task ResetAsync(IServiceProvider services)
    {
        services.GetRequiredService<PreparedProcessDefinitionCache>().Clear();

        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await ResetDatabaseAsync(db);

        var redis = services.GetRequiredService<IConnectionMultiplexer>();
        var database = redis.GetDatabase().Database;
        await redis.GetServer(redis.GetEndPoints().Single()).FlushDatabaseAsync(database);
    }

    private static Task ResetDatabaseAsync(AppDbContext db)
    {
        var sql = db.GetService<ISqlGenerationHelper>();
        var tables = db
            .Model
            .GetEntityTypes()
            .Select(entity => sql.DelimitIdentifier(entity.GetTableName()!, entity.GetSchema() ?? "public"))
            .Distinct();
        // Identifiers come from EF metadata and are escaped by the provider.
        var statement = $"TRUNCATE TABLE {string.Join(", ", tables)} RESTART IDENTITY CASCADE";

        return db.Database.ExecuteSqlRawAsync(statement);
    }

    private static async Task CreateSchemaAsync(string connectionString, CancellationToken ct)
    {
        var options = new DbContextOptionsBuilder<AppDbContext>().UseNpgsql(connectionString).Options;
        await using var db = new AppDbContext(options);
        await db.Database.EnsureCreatedAsync(ct);
    }

    private async Task CreateTopicsAsync()
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig
                { BootstrapServers = BootstrapServers }).Build();
        await admin.CreateTopicsAsync(
            new[] { "billing", "shipping", "refund", "undo-a", "undo-b", "high", "low" }
                .Select(service => new TopicSpecification
                {
                    Name = $"process.commands.{service}",
                    NumPartitions = 1,
                    ReplicationFactor = 1
                }),
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
    }
}