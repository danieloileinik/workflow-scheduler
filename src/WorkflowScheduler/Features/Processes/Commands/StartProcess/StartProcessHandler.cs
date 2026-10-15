using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Features.Steps.Strategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public class StartProcessHandler(
    AppDbContext db,
    CachedProcessDefinitionProvider definitions,
    StepExecutor executor)
    : IRequestHandler<StartProcessCommand, ErrorOr<StartProcessResponse>>
{
    public async Task<ErrorOr<StartProcessResponse>> Handle(StartProcessCommand request, CancellationToken ct)
    {
        var hash = Convert.ToHexString(
            SHA256.HashData(
                Encoding.UTF8.GetBytes(
                    JsonSerializer.Serialize(new { request.DefinitionId, request.BusinessKey, request.InputData }))));
        var previous = await db.StartRequests.FindAsync([request.IdempotencyKey], ct);
        if (previous is not null)
            return previous.RequestHash == hash
                ? new StartProcessResponse(previous.ProcessInstanceId)
                : Error.Conflict("Process.IdempotencyKeyConflict", "Idempotency key was used with a different request");
        var processDefinition = await definitions.GetOrLoadAsync(request.DefinitionId, ct);
        if (processDefinition is null) return ProcessErrors.DefinitionNotFound;

        var firstStepResult = processDefinition.GetFirstStep();
        if (firstStepResult.IsError) return firstStepResult.Errors;

        var processInstance = new ProcessInstance(request.DefinitionId, request.BusinessKey, request.InputData);

        var startResult = processInstance.Start(firstStepResult.Value.Id);
        if (startResult.IsError) return startResult.Errors;

        var runResult = await executor.RunAsync(
            processDefinition,
            processInstance,
            firstStepResult.Value,
            startResult.Value,
            ct);
        if (runResult.IsError) return runResult.Errors;

        db.ProcessInstances.Add(processInstance);
        db.StartRequests.Add(
            new StartRequest
            {
                Id = request.IdempotencyKey,
                ProcessInstanceId = processInstance.Id,
                RequestHash = hash
            });

        return new StartProcessResponse(processInstance.Id);
    }
}