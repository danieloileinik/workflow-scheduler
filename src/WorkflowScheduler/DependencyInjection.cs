using FluentValidation;
using StackExchange.Redis;
using WorkflowScheduler.Common.Behaviors;
using WorkflowScheduler.Features.Processes;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Caching;
using WorkflowScheduler.Infrastructure.Messaging;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler;

public static class DependencyInjection
{
    extension(IServiceCollection services)
    {
        public void AddWorkflowScheduler(IConfiguration configuration)
        {
            services
                .AddHealthChecks()
                .AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"], timeout: TimeSpan.FromSeconds(3));

            services.AddDbContext<AppDbContext>(opt =>
                opt.UseNpgsql(configuration.GetConnectionString("Postgres")));

            var redisConnection = configuration.GetConnectionString("Redis");
            if (string.IsNullOrWhiteSpace(redisConnection))
            {
                services.AddSingleton<IProcessDefinitionCache, DisabledProcessDefinitionCache>();
            }
            else
            {
                services.AddSingleton<IConnectionMultiplexer>(_ =>
                {
                    var options = ConfigurationOptions.Parse(redisConnection);
                    options.AbortOnConnectFail = false;

                    return ConnectionMultiplexer.Connect(options);
                });
                services.AddScoped<IProcessDefinitionCache, ProcessDefinitionCache>();
            }

            services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
            services.AddOptions<CallRecoveryOptions>()
                .Bind(configuration.GetSection("CallRecovery"))
                .Validate(options => options.CallbackTimeout > TimeSpan.Zero
                                     && options.CallbackTimeout <= TimeSpan.FromDays(30),
                    "CallbackTimeout must be positive and no greater than 30 days.")
                .ValidateOnStart();
            services.AddSingleton<IMessagePublisher, KafkaPublisher>();
            services.AddScoped<OutboxDispatcher>();
            services.AddHostedService<OutboxWorker>();

            services.AddHostedService<RetryWorker>();

            services.AddWorkflowApplication();
        }

        private void AddWorkflowApplication()
        {
            services.AddSingleton<ProcessDefinitionMemoryCache>();
            services.AddScoped<CachedProcessDefinitionProvider>();
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
        }
    }
}
