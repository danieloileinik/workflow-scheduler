using FluentValidation;
using WorkflowScheduler.Contracts.Grpc;

namespace WorkflowScheduler.Features.Steps.Commands.CompleteStep;

public class CompleteStepValidator : AbstractValidator<CompleteStepCommand>
{
    public CompleteStepValidator()
    {
        RuleFor(x => x.ProcessInstanceId).NotEmpty();
        RuleFor(x => x.StepDefinitionId).NotEmpty();
        RuleFor(x => x.StepInstanceId).NotEmpty();
        RuleFor(x => x.Attempt).GreaterThanOrEqualTo(0);
        RuleFor(x => x.CorrelationId).NotEmpty().MaximumLength(256);
        RuleFor(x => x.OutputData)
            .Must(WorkflowProtocol.OutputFitsMessage)
            .WithMessage("Step output exceeds the callback message size limit.");
    }
}