using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public class SendEventValidator : AbstractValidator<SendEventCommand>
{
    public SendEventValidator()
    {
        RuleFor(x => x.ProcessInstanceId).NotEmpty();
        RuleFor(x => x.EventName).NotEmpty().MaximumLength(250);
        RuleFor(x => x.CorrelationId).NotEmpty().MaximumLength(256);
    }
}