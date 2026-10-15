using ErrorOr;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class StepExecutor(IEnumerable<IStepStrategy> strategies)
{
    public Task<ErrorOr<Success>> ExecuteAsync(ProcessDefinition definition,
        ProcessInstance instance,
        Guid nextStepId,
        CancellationToken ct)
    {
        var next = definition.FindStep(nextStepId);
        if (next is null) return Task.FromResult<ErrorOr<Success>>(ProcessErrors.StepNotFound);
        var move = instance.MoveToStep(nextStepId);
        return move.IsError
            ? Task.FromResult<ErrorOr<Success>>(move.Errors)
            : RunAsync(definition, instance, next, move.Value, ct);
    }

    public async Task<ErrorOr<Success>> RunAsync(ProcessDefinition definition,
        ProcessInstance instance,
        StepDefinition stepDefinition,
        StepInstance step,
        CancellationToken ct)
    {
        var visited = new HashSet<Guid>();
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            if (!visited.Add(stepDefinition.Id))
                return Error.Validation("ProcessDefinition.Cycle", "Automatic step chain contains a cycle");
            var strategy = strategies.FirstOrDefault(s => s.StepType == stepDefinition.Type);
            if (strategy is null) return ProcessErrors.StepTypeMismatch;
            var start = step.MarkRunning();
            if (start.IsError) return start;
            var result = await strategy.ExecuteAsync(instance, step, stepDefinition, ct);
            var execution = result.State switch
            {
                StepExecutionState.Completed => step.Complete(),
                StepExecutionState.Running => Result.Success,
                StepExecutionState.Waiting => step.MarkWaiting(),
                _ => Error.Validation("ProcessStep.InvalidExecutionState", "Unknown step execution state")
            };
            if (execution.IsError) return execution;
            if (step.IsCompensation) return Result.Success;
            if (result.State != StepExecutionState.Completed) return instance.Wait();

            var nextId = result.OverrideNextStepId ?? stepDefinition.NextStepId;
            if (nextId is null) return instance.Complete();
            var next = definition.FindStep(nextId.Value);
            if (next is null) return ProcessErrors.StepNotFound;
            var move = instance.MoveToStep(nextId.Value);
            if (move.IsError) return move.Errors;
            stepDefinition = next;
            step = move.Value;
        }
    }
}