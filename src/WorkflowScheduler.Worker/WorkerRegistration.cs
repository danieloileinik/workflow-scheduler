namespace WorkflowScheduler.Worker;

internal sealed record WorkerRegistration(
    WorkflowWorkerOptions Options,
    IReadOnlyDictionary<string, HandlerRegistration> Handlers);