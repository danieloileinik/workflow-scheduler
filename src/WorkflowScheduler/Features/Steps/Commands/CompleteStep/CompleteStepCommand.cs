using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Steps.Commands.CompleteStep;

public record CompleteStepCommand(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    bool Success,
    string? OutputData,
    string CorrelationId,
    Guid StepInstanceId,
    int Attempt = 0) : IRequest<ErrorOr<Success>>, IProcessCommand;