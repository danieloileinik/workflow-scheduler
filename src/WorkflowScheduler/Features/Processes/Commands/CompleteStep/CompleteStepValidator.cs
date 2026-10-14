using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.CompleteStep;

public class CompleteStepValidator : AbstractValidator<CompleteStepCommand>
{
    public CompleteStepValidator()
    {
        RuleFor(x => x.ProcessInstanceId).NotEmpty();
        RuleFor(x => x.StepDefinitionId).NotEmpty();
        RuleFor(x => x.StepInstanceId).NotEmpty();
        RuleFor(x => x.Attempt).GreaterThanOrEqualTo(0);
        RuleFor(x => x.CorrelationId).NotEmpty().MaximumLength(256);
    }
}