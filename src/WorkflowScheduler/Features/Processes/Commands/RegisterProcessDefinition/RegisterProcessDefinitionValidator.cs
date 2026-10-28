using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class RegisterProcessDefinitionValidator : AbstractValidator<RegisterProcessDefinitionCommand>
{
    private static readonly ProcessDefinitionStructureValidator Structure = new();

    public RegisterProcessDefinitionValidator()
    {
        RuleFor(command => command)
            .Custom((command, context) =>
            {
                foreach (var failure in Structure.Validate(command.Definition).Errors)
                    context.AddFailure(failure);
            });
    }
}