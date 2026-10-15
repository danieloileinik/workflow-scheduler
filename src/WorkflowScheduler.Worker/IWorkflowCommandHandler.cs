namespace WorkflowScheduler.Worker;

public interface IWorkflowCommandHandler<in TInput, TOutput>
{
    Task<TOutput> HandleAsync(TInput input, WorkflowCommandContext context, CancellationToken cancellationToken);
}