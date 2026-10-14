using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Commands.RetrySteps;

public record RetryStepsCommand(Guid ProcessInstanceId) : IRequest<ErrorOr<Success>>, IProcessCommand;