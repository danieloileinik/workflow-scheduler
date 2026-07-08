using ErrorOr;
using MediatR;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public record CancelProcessCommand(Guid ProcessInstanceId) : IRequest<ErrorOr<Success>>;