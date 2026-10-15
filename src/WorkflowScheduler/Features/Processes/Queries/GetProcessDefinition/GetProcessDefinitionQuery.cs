using MediatR;
using WorkflowScheduler.Common.Abstractions;

namespace WorkflowScheduler.Features.Processes.Queries.GetProcessDefinition;

public record GetProcessDefinitionQuery(string Name, int Version) : IRequest<Guid?>, IQuery;