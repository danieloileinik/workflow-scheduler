using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace WorkflowScheduler.Worker;

public sealed class WorkflowWorkerBuilder
{
    private readonly Dictionary<string, HandlerRegistration> _handlers = new(StringComparer.Ordinal);
    public IServiceCollection Services { get; }

    internal WorkflowWorkerBuilder(IServiceCollection services) => Services = services;

    public WorkflowWorkerBuilder Handle<TInput, TOutput, THandler>(string command)
        where THandler : class, IWorkflowCommandHandler<TInput, TOutput>
    {
        Register(command, async (services, input, context, json, ct) =>
        {
            var typed = ReadInput<TInput>(input, json);
            var output = await services.GetRequiredService<THandler>().HandleAsync(typed, context, ct);
            return JsonSerializer.SerializeToElement(output, json);
        });
        Services.TryAddScoped<THandler>();
        return this;
    }

    public WorkflowWorkerBuilder Handle<TInput, TOutput>(string command,
        Func<TInput, WorkflowCommandContext, CancellationToken, Task<TOutput>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Register(command, async (_, input, context, json, ct) =>
            JsonSerializer.SerializeToElement(await handler(ReadInput<TInput>(input, json), context, ct), json));
        return this;
    }

    internal IReadOnlyDictionary<string, HandlerRegistration> Snapshot()
    {
        if (_handlers.Count == 0) throw new InvalidOperationException("Register at least one workflow command handler.");
        return new Dictionary<string, HandlerRegistration>(_handlers, StringComparer.Ordinal);
    }

    private void Register(string command, HandlerRegistration handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(command);
        if (!_handlers.TryAdd(command, handler))
            throw new ArgumentException($"A handler for '{command}' is already registered.", nameof(command));
    }

    private static T ReadInput<T>(string? input, JsonSerializerOptions json) =>
        JsonSerializer.Deserialize<T>(input ?? "null", json)
        ?? throw new JsonException("Workflow input cannot be null.");
}

internal delegate Task<JsonElement> HandlerRegistration(IServiceProvider services, string? input,
    WorkflowCommandContext context, JsonSerializerOptions json, CancellationToken ct);

internal sealed record WorkerRegistration(WorkflowWorkerOptions Options, IReadOnlyDictionary<string, HandlerRegistration> Handlers);

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
