using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.ResumeCompensation;

public sealed class ResumeCompensationValidator : AbstractValidator<ResumeCompensationCommand>
{
    public ResumeCompensationValidator()
    {
        RuleFor(command => command.ProcessInstanceId).NotEmpty();
        RuleFor(command => command.FailedStepInstanceId).NotEmpty();
    }
}
