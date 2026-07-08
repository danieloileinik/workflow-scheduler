using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class RegisterProcessDefinitionValidator : AbstractValidator<RegisterProcessDefinitionCommand>
{
    public RegisterProcessDefinitionValidator()
    {
        RuleFor(x => x.Name).NotEmpty().MaximumLength(256);
        RuleFor(x => x.Version).GreaterThan(0);
        RuleFor(x => x.Steps).NotEmpty();
        RuleForEach(x => x.Steps)
            .ChildRules(step =>
            {
                step.RuleFor(s => s.Id).NotEmpty();
                step.RuleFor(s => s.Name).NotEmpty().MaximumLength(256);
                step.RuleFor(s => s.Metadata).NotEmpty();
            });
    }
}