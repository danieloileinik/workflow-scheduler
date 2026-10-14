namespace WorkflowScheduler.Common.Abstractions;

public interface IProcessCommand
{
    Guid ProcessInstanceId { get; }
}