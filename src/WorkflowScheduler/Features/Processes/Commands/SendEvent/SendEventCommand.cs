using ErrorOr;
using MediatR;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public record SendEventCommand(
    Guid ProcessInstanceId,
    string EventName,
    string? Payload,
    string CorrelationId) : IRequest<ErrorOr<Success>>;