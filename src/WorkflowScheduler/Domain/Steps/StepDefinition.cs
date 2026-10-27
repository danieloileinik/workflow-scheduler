using System.ComponentModel.DataAnnotations.Schema;

namespace WorkflowScheduler.Domain.Steps;

public class StepDefinition(
    Guid id,
    Guid processDefinitionId,
    string name,
    StepType type,
    string metadata,
    Guid? nextStepId,
    Guid? compensationStepId)
{
    private StepMetadata? _parsedMetadata;

    public StepDefinition(
        Guid processDefinitionId,
        string name,
        StepType type,
        string metadata,
        Guid? nextStepId = null,
        Guid? compensationStepId = null)
        : this(Guid.NewGuid(), processDefinitionId, name, type, metadata, nextStepId, compensationStepId)
    {
    }

    public int Position { get; private set; }

    public Guid Id { get; init; } = id;
    public Guid ProcessDefinitionId { get; init; } = processDefinitionId;
    public string Name { get; init; } = name;
    public StepType Type { get; init; } = type;
    public string Metadata { get; init; } = metadata;

    [NotMapped]
    public StepMetadata ParsedMetadata => _parsedMetadata ??= StepMetadata.Parse(Type, Metadata);

    public Guid? NextStepId { get; init; } = nextStepId;
    public Guid? CompensationStepId { get; init; } = compensationStepId;

    public void SetPosition(int position)
    {
        Position = position;
    }
}
