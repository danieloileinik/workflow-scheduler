namespace WorkflowScheduler.Common.Abstractions;

public interface IIdempotentCommand
{
    Guid IdempotencyKey { get; }
}