using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public class StartProcessValidator : AbstractValidator<StartProcessCommand>
{
    public StartProcessValidator()
    {
        RuleFor(x => x.IdempotencyKey).NotEmpty();
        RuleFor(x => x.DefinitionId).NotEmpty();
    }
}