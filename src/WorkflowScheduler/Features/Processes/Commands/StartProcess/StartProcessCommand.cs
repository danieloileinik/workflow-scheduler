using ErrorOr;
using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public record StartProcessCommand(
    Guid DefinitionId,
    Guid IdempotencyKey,
    string BusinessKey,
    string InputData
) : IRequest<ErrorOr<StartProcessResponse>>, IIdempotentCommand;

public record StartProcessResponse(Guid ProcessInstanceId);