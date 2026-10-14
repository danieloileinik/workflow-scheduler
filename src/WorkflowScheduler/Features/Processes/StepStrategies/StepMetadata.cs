using System.Text.Json;

namespace WorkflowScheduler.Features.Processes.StepStrategies;

public static class StepMetadata
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);
}