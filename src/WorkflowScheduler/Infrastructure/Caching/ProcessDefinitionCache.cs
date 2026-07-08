using System.Text.Json;
using StackExchange.Redis;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes;

namespace WorkflowScheduler.Infrastructure.Caching;

public class ProcessDefinitionCache(IConnectionMultiplexer redis) : IProcessDefinitionCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(30);
    private IDatabase Db => redis.GetDatabase();

    public async Task<ProcessDefinition?> GetAsync(Guid definitionId, CancellationToken ct = default)
    {
        var value = await Db.StringGetAsync(Key(definitionId));
        if (value.IsNullOrEmpty) return null;

        var snapshot = JsonSerializer.Deserialize<DefinitionSnapshot>((string)value!)!;
        var definition = new ProcessDefinition(snapshot.Id, snapshot.Name, snapshot.Version);

        foreach (var step in snapshot.Steps)
            definition.AddStep(
                new StepDefinition(
                    step.Id,
                    snapshot.Id,
                    step.Name,
                    step.Type,
                    step.Metadata,
                    step.NextStepId,
                    step.CompensationStepId));

        return definition;
    }

    public Task SetAsync(ProcessDefinition definition, CancellationToken ct = default)
    {
        var snapshot = new DefinitionSnapshot(
            definition.Id,
            definition.Name,
            definition.Version,
            definition
                .Steps
                .Select(s => new StepSnapshot(s.Id, s.Name, s.Type, s.Metadata, s.NextStepId, s.CompensationStepId))
                .ToList());

        return Db.StringSetAsync(Key(definition.Id), JsonSerializer.Serialize(snapshot), Ttl);
    }

    private static string Key(Guid id)
    {
        return $"proc-def:{id}";
    }

    private record StepSnapshot(
        Guid Id,
        string Name,
        StepType Type,
        string Metadata,
        Guid? NextStepId,
        Guid? CompensationStepId);

    private record DefinitionSnapshot(Guid Id, string Name, int Version, List<StepSnapshot> Steps);
}