using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Commands.ResumeCompensation;

public sealed record ResumeCompensationCommand(Guid ProcessInstanceId, Guid FailedStepInstanceId)
    : IRequest<ErrorOr<Success>>, IProcessCommand;
