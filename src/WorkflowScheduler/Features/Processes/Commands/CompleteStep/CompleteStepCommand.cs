using ErrorOr;
using MediatR;

namespace WorkflowScheduler.Features.Processes.Commands.CompleteStep;

public record CompleteStepCommand(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    bool Success,
    string? OutputData,
    string CorrelationId) : IRequest<ErrorOr<Success>>;