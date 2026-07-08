using ErrorOr;

namespace WorkflowScheduler.Domain.Processes;

public static class ProcessErrors
{
    public static Error NotFound =>
        Error.NotFound("Process.NotFound", "Process not found");

    public static Error DefinitionNotFound =>
        Error.NotFound("ProcessDefinition.NotFound", "Process definition not found");

    public static Error StepNotFound =>
        Error.NotFound("ProcessStep.NotFound", "Process step not found");

    public static Error NoStepsDefined =>
        Error.Validation("ProcessDefinition.NoSteps", "Process definition has no steps defined");

    public static Error AlreadyCompleted =>
        Error.Conflict("Process.AlreadyCompleted", "Process is already completed");

    public static Error AlreadyCancelled =>
        Error.Conflict("Process.AlreadyCancelled", "Process is already cancelled");

    public static Error StepAlreadyCompleted =>
        Error.Conflict("ProcessStep.AlreadyCompleted", "Step is already completed");

    public static Error DuplicateEvent =>
        Error.Conflict("Process.DuplicateEvent", "Event with this correlation id was already processed");

    public static Error DefinitionVersionConflict =>
        Error.Conflict("ProcessDefinition.VersionConflict", "A definition with this name and version already exists");

    public static Error LockNotAcquired =>
        Error.Conflict("Process.LockNotAcquired", "Could not acquire lock, another operation is in progress");

    public static Error NotWaiting =>
        Error.Conflict("Process.NotWaiting", "Process is not in waiting state");

    public static Error EventNameMismatch =>
        Error.Conflict(
            "Process.EventNameMismatch",
            "Event name does not match the expected event for the current step");

    public static Error StepTypeMismatch =>
        Error.Conflict("Process.StepTypeMismatch", "Current step type does not support this operation");

    public static Error InvalidTransition(ProcessStatus from, ProcessStatus to)
    {
        return Error.Validation("Process.InvalidTransition", $"Cannot transition from {from} to {to}");
    }
}