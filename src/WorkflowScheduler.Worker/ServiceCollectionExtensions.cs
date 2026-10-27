using Microsoft.Extensions.DependencyInjection;

namespace WorkflowScheduler.Worker;

public static class ServiceCollectionExtensions
{
    public static WorkflowWorkerBuilder AddWorkflowSchedulerWorker(this IServiceCollection services,
        Action<WorkflowWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(WorkerRegistration)))
            throw new InvalidOperationException("Register one workflow worker per host; add its commands with Handle.");

        var options = new WorkflowWorkerOptions();
        configure(options);

        var snapshot = options.CopyAndValidate();
        var builder = new WorkflowWorkerBuilder(services);
        services.AddSingleton(_ => new WorkerRegistration(snapshot, builder.Snapshot()));
        services.AddSingleton<WorkflowMessageProcessor>();
        services.AddHostedService<WorkflowWorker>();

        return builder;
    }
}