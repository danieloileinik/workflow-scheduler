using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public class CancelProcessValidator : AbstractValidator<CancelProcessCommand>
{
    public CancelProcessValidator()
    {
        RuleFor(x => x.ProcessInstanceId).NotEmpty();
    }
}