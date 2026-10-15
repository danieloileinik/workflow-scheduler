using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Steps.Queries.IsStepActive;

public record IsStepActiveQuery(
    Guid ProcessInstanceId,
    Guid StepDefinitionId,
    Guid StepInstanceId,
    int Attempt,
    bool IsCompensation) : IRequest<bool>, IQuery;