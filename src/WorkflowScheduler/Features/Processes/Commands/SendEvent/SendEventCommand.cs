using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Commands.SendEvent;

public record SendEventCommand(
    Guid ProcessInstanceId,
    string EventName,
    string? Payload,
    string CorrelationId) : IRequest<ErrorOr<Success>>, IProcessCommand;