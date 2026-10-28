using FluentValidation;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class RegisterProcessDefinitionValidator : AbstractValidator<RegisterProcessDefinitionCommand>
{
    public RegisterProcessDefinitionValidator(IValidator<ProcessDefinition> structure)
    {
        RuleFor(command => command)
            .Custom((command, context) =>
            {
                foreach (var failure in structure.Validate(command.Definition).Errors)
                    context.AddFailure(failure);
            });
    }
}
