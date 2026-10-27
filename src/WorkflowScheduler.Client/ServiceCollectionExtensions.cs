using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace WorkflowScheduler.Client;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddWorkflowSchedulerClient(this IServiceCollection services,
        Action<WorkflowClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new WorkflowClientOptions();
        configure(options);
        WorkflowSchedulerClient.ValidateOptions(options, true);
        services.TryAddSingleton<IWorkflowSchedulerClient>(_ => new WorkflowSchedulerClient(options));

        return services;
    }
}