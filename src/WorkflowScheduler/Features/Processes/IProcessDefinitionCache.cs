using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Features.Processes;

public interface IProcessDefinitionCache
{
    Task<ProcessDefinition?> GetAsync(Guid definitionId, CancellationToken ct = default);
    Task SetAsync(ProcessDefinition definition, CancellationToken ct = default);
}