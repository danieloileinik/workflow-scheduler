using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.CompleteStep;

public class CompleteStepValidator : AbstractValidator<CompleteStepCommand>
{
    public CompleteStepValidator()
    {
        RuleFor(x => x.ProcessInstanceId).NotEmpty();
        RuleFor(x => x.StepDefinitionId).NotEmpty();
    }
}