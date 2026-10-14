using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ErrorOr;
using MediatR;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Features.Processes.StepStrategies;
using WorkflowScheduler.Infrastructure.Persistence;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public class StartProcessHandler(
    AppDbContext db,
    IProcessDefinitionCache cache,
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
        var processDefinition = await GetDefinitionAsync(request.DefinitionId, ct);
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

    private async Task<ProcessDefinition?> GetDefinitionAsync(Guid id, CancellationToken ct)
    {
        var cached = await cache.GetAsync(id, ct);
        if (cached is not null) return cached;

        var definition = await db
            .ProcessDefinitions
            .Include(d => d.Steps)
            .FirstOrDefaultAsync(d => d.Id == id, ct);

        if (definition is not null)
            await cache.SetAsync(definition, ct);

        return definition;
    }
}