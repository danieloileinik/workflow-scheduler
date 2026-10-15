using ErrorOr;

namespace WorkflowScheduler.Domain.Steps;

public static class StepErrors
{
    public static Error InvalidTransition(StepStatus from, StepStatus to)
    {
        return Error.Conflict("ProcessStep.InvalidTransition", $"Cannot transition from {from} to {to}");
    }
}