namespace WorkflowScheduler.Domain.Steps;

public class ProcessStepDefinition(
    Guid id,
    Guid processDefinitionId,
    string name,
    StepType stepType,
    string metadata,
    Guid? nextStepId,
    Guid? compensationStepId)
{
    public ProcessStepDefinition(
        Guid processDefinitionId,
        string name,
        StepType stepType,
        string metadata,
        Guid? nextStepId = null,
        Guid? compensationStepId = null)
        : this(Guid.NewGuid(), processDefinitionId, name, stepType, metadata, nextStepId, compensationStepId)
    {
    }

    public Guid Id { get; init; } = id;
    public Guid ProcessDefinitionId { get; init; } = processDefinitionId;
    public string Name { get; init; } = name;
    public StepType StepType { get; init; } = stepType;
    public string Metadata { get; init; } = metadata;
    public Guid? NextStepId { get; init; } = nextStepId;
    public Guid? CompensationStepId { get; init; } = compensationStepId;
}