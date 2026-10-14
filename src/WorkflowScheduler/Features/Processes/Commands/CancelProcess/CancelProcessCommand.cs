using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Commands.CancelProcess;

public record CancelProcessCommand(Guid ProcessInstanceId) : IRequest<ErrorOr<Success>>, IProcessCommand;