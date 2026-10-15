using System.Text.Json;

namespace WorkflowScheduler.Features.Steps.Strategies;

public static class StepMetadata
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);
}