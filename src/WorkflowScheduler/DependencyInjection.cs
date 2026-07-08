using StackExchange.Redis;
using WorkflowScheduler.Common.Abstractions;
using WorkflowScheduler.Common.Behaviors;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Features.Processes.StepStrategies;
using WorkflowScheduler.Infrastructure.Caching;
using WorkflowScheduler.Infrastructure.Messaging;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler;

public static class DependencyInjection
{
    public static IServiceCollection AddWorkflowScheduler(this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddDbContext<AppDbContext>(opt =>
            opt.UseNpgsql(configuration.GetConnectionString("Postgres")));

        services.AddSingleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(configuration.GetConnectionString("Redis")!));

        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        services.AddSingleton<KafkaPublisher>();
        services.AddHostedService<OutboxWorker>();

        services.AddScoped<IProcessLock, DistributedLock>();
        services.AddScoped<IIdempotencyCache, IdempotencyCache>();
        services.AddScoped<IProcessDefinitionCache, ProcessDefinitionCache>();

        services.AddScoped<IStepStrategy, CallStepStrategy>();
        services.AddScoped<IStepStrategy, WaitEventStepStrategy>();
        services.AddScoped<IStepStrategy, ConditionStepStrategy>();
        services.AddScoped<StepExecutor>();

        services.AddValidatorsFromAssembly(typeof(DependencyInjection).Assembly);

        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(DependencyInjection).Assembly);
            cfg.AddOpenBehavior(typeof(ValidationBehavior<,>));
            cfg.AddOpenBehavior(typeof(IdempotencyBehavior<,>));
            cfg.AddOpenBehavior(typeof(TransactionBehavior<,>));
        });

        return services;
    }
}