using FluentValidation;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;

public class RegisterProcessDefinitionValidator : AbstractValidator<RegisterProcessDefinitionCommand>
{
    private static readonly ProcessDefinitionStructureValidator Structure = new();

    public RegisterProcessDefinitionValidator()
    {
        RuleFor(command => command)
            .Custom((command, context) =>
            {
                var definition = new ProcessDefinition(command.Name, command.Version);
                if (command.Steps is not null)
                    foreach (var step in command.Steps)
                        definition.AddStep(
                            new StepDefinition(
                                step.Id,
                                definition.Id,
                                step.Name,
                                step.Type,
                                step.Metadata,
                                step.NextStepId,
                                step.CompensationStepId));

                foreach (var failure in Structure.Validate(definition).Errors)
                    context.AddFailure(failure);
            });
    }
}