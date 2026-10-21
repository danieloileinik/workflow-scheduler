using System.Text.Json;

namespace WorkflowScheduler.Worker;

internal delegate Task<JsonElement> HandlerRegistration(IServiceProvider services,
    string? input,
    WorkflowCommandContext context,
    JsonSerializerOptions json,
    CancellationToken ct);