using MediatR;
using GrpcProcessesBase = WorkflowScheduler.Grpc.Processes.ProcessesBase;

namespace WorkflowScheduler.Features.Processes.Endpoints;

public partial class ProcessService(IMediator mediator) : GrpcProcessesBase;