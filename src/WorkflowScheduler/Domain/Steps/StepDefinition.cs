using System.ComponentModel.DataAnnotations.Schema;

namespace WorkflowScheduler.Domain.Steps;

public sealed class StepDefinition(
    Guid id,
    Guid processDefinitionId,
    string name,
    StepType type,
    string metadata,
    Guid? nextStepId = null,
    Guid? compensationStepId = null,
    int position = 0)
{
    private readonly Lazy<StepMetadata> _parsedMetadata = new(() => StepMetadata.Parse(type, metadata));

    public Guid Id { get; } = id;
    public Guid ProcessDefinitionId { get; } = processDefinitionId;
    public int Position { get; } = position;
    public string Name { get; } = name;
    public StepType Type { get; } = type;
    public string Metadata { get; } = metadata;
    public Guid? NextStepId { get; } = nextStepId;
    public Guid? CompensationStepId { get; } = compensationStepId;

    [NotMapped]
    public StepMetadata ParsedMetadata => _parsedMetadata.Value;
}
