using FluentValidation;
using StackExchange.Redis;
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
        {
            var options = ConfigurationOptions.Parse(configuration.GetConnectionString("Redis")!);
            options.AbortOnConnectFail = false;
            return ConnectionMultiplexer.Connect(options);
        });

        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        services.AddSingleton<IMessagePublisher, KafkaPublisher>();
        services.AddScoped<OutboxDispatcher>();
        services.AddHostedService<OutboxWorker>();

        services.AddScoped<IProcessDefinitionCache, ProcessDefinitionCache>();
        services.AddHostedService<RetryWorker>();
        return services.AddWorkflowApplication();
    }

    public static IServiceCollection AddWorkflowApplication(this IServiceCollection services)
    {
        services.AddScoped<SagaCoordinator>();

        services.AddScoped<IStepStrategy, CallStepStrategy>();
        services.AddScoped<IStepStrategy, WaitEventStepStrategy>();
        services.AddScoped<IStepStrategy, ConditionStepStrategy>();
        services.AddScoped<StepExecutor>();

        services.AddValidatorsFromAssembly(typeof(DependencyInjection).Assembly);

        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(DependencyInjection).Assembly);
            cfg.AddOpenBehavior(typeof(ValidationBehavior<,>));
            cfg.AddOpenBehavior(typeof(TransactionBehavior<,>));
        });

        return services;
    }
}