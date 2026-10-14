using System.Text.Json;
using StackExchange.Redis;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Features.Processes;

namespace WorkflowScheduler.Infrastructure.Caching;

public class ProcessDefinitionCache(IConnectionMultiplexer redis, ILogger<ProcessDefinitionCache> logger)
    : IProcessDefinitionCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(30);
    private IDatabase Db => redis.GetDatabase();

    public async Task<ProcessDefinition?> GetAsync(Guid definitionId, CancellationToken ct = default)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var value = await Db.StringGetAsync(Key(definitionId)).WaitAsync(ct);
            if (value.IsNullOrEmpty) return null;

            var snapshot = JsonSerializer.Deserialize<DefinitionSnapshot>(value.ToString());
            if (snapshot is null || snapshot.Id != definitionId || snapshot.Steps is null
                || string.IsNullOrWhiteSpace(snapshot.Name) || snapshot.Version <= 0) return null;
            var definition = new ProcessDefinition(snapshot.Id, snapshot.Name, snapshot.Version)
                { CreatedAt = snapshot.CreatedAt };

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
        catch (Exception ex) when (ex is RedisException or JsonException)
        {
            logger.LogWarning(ex, "Definition cache unavailable or invalid for {DefinitionId}", definitionId);
            return null;
        }
    }

    public async Task SetAsync(ProcessDefinition definition, CancellationToken ct = default)
    {
        var snapshot = new DefinitionSnapshot(
            definition.Id,
            definition.Name,
            definition.Version,
            definition.CreatedAt,
            [
                .. definition
                    .Steps
                    .OrderBy(s => s.Position)
                    .Select(s => new StepSnapshot(s.Id, s.Name, s.Type, s.Metadata, s.NextStepId, s.CompensationStepId))
            ]);

        try
        {
            ct.ThrowIfCancellationRequested();
            await Db.StringSetAsync(Key(definition.Id), JsonSerializer.Serialize(snapshot), Ttl).WaitAsync(ct);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Definition cache write failed for {DefinitionId}", definition.Id);
        }
    }

    private static string Key(Guid id)
    {
        return $"proc-def:v2:{id}";
    }

    private record StepSnapshot(
        Guid Id,
        string Name,
        StepType Type,
        string Metadata,
        Guid? NextStepId,
        Guid? CompensationStepId);

    private record DefinitionSnapshot(Guid Id, string Name, int Version, DateTime CreatedAt, List<StepSnapshot> Steps);
}