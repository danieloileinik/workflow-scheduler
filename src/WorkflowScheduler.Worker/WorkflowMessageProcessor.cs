using System.Text.Json;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Grpc;

namespace WorkflowScheduler.Worker;

internal sealed class WorkflowMessageProcessor(
    WorkerRegistration registration,
    IServiceScopeFactory scopes,
    IWorkflowSchedulerClient client,
    ILogger<WorkflowMessageProcessor> logger)
{
    private static readonly JsonSerializerOptions EnvelopeJson = new(JsonSerializerDefaults.Web);
    private readonly WorkflowWorkerOptions _options = registration.Options;
    private readonly WorkerRetry _retry = new(registration.Options, logger);

    public static ExecuteStepCommand ReadCommand(string? payload, string? key)
    {
        var command = JsonSerializer.Deserialize<ExecuteStepCommand>(payload ?? "null", EnvelopeJson);
        if (command is null || command.ProcessInstanceId == Guid.Empty || command.StepDefinitionId == Guid.Empty
            || command.StepInstanceId == Guid.Empty || command.Attempt < 0 || string.IsNullOrWhiteSpace(command.Command)
            || string.IsNullOrWhiteSpace(command.CorrelationId) || command.CorrelationId.Length > 256
            || !Guid.TryParse(key, out var processId) || processId != command.ProcessInstanceId)
            throw new JsonException("Invalid step command identity, attempt, correlation or Kafka process key.");
        return command;
    }

    public async Task ProcessAsync(ExecuteStepCommand command, CancellationToken ct)
    {
        if (!await IsActiveAsync(command, ct))
        {
            logger.LogInformation(
                "Skipping inactive execution {StepInstanceId}, attempt {Attempt}",
                command.StepInstanceId,
                command.Attempt);
            return;
        }

        var context = new WorkflowCommandContext(
            new StepExecution(
                command.ProcessInstanceId,
                command.StepDefinitionId,
                command.StepInstanceId,
                command.Attempt),
            command.Command,
            command.CorrelationId,
            command.IsCompensation);
        JsonElement output = default;
        string? failure = null;
        if (!registration.Handlers.TryGetValue(command.Command, out var handler))
        {
            failure = $"No handler registered for command '{command.Command}'.";
        }
        else
        {
            // Keep scoped dependencies alive until an asynchronous handler has finished.
            await using var scope = scopes.CreateAsyncScope();
            using var handlerTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            handlerTimeout.CancelAfter(_options.HandlerTimeout);
            try
            {
                output = await handler(
                    scope.ServiceProvider,
                    command.InputData,
                    context,
                    _options.JsonSerializerOptions,
                    handlerTimeout.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failure = handlerTimeout.IsCancellationRequested ? "Command handler timed out." : exception.Message;
                if (string.IsNullOrWhiteSpace(failure)) failure = exception.GetType().Name;
                logger.LogWarning(
                    exception,
                    "Command {Command} failed for {StepInstanceId}, attempt {Attempt}",
                    command.Command,
                    command.StepInstanceId,
                    command.Attempt);
            }
        }

        if (failure is null && !WorkflowProtocol.OutputFitsMessage(JsonSerializer.Serialize(output)))
            failure = "Command output exceeds the callback message size limit.";
        failure = failure?[..Math.Min(failure.Length, 2048)];
        ct.ThrowIfCancellationRequested();

        await _retry.RunAsync(
            "Workflow callback",
            async token =>
            {
                try
                {
                    if (failure is null)
                        try
                        {
                            await client.CompleteStepAsync(
                                context.Execution,
                                output,
                                context.CallbackCorrelationId,
                                token);
                        }
                        catch (ArgumentException exception) when (exception.ParamName == "output")
                        {
                            // The client may use a different JSON encoder; its final byte limit is authoritative.
                            failure = "Command output exceeds the callback message size limit.";
                            await client.FailStepAsync(
                                context.Execution,
                                failure,
                                context.CallbackCorrelationId,
                                token);
                        }
                    else
                        await client.FailStepAsync(context.Execution, failure, context.CallbackCorrelationId, token);
                }
                catch (WorkflowClientException exception) when (
                    exception.StatusCode is StatusCode.FailedPrecondition or StatusCode.NotFound)
                {
                    // The previous RPC may have committed before its response was lost. Reconcile
                    // with persisted state instead of treating every conflict as an acknowledgement.
                    if (await IsActiveAsync(command, token)) throw;
                    logger.LogInformation(
                        "Execution {StepInstanceId}, attempt {Attempt} is no longer active",
                        command.StepInstanceId,
                        command.Attempt);
                }

                return true;
            },
            ct);
    }

    private async Task<bool> IsActiveAsync(ExecuteStepCommand command, CancellationToken ct)
    {
        try
        {
            return await _retry.RunAsync(
                "Workflow status",
                token => client.IsStepActiveAsync(
                    new StepExecution(
                        command.ProcessInstanceId,
                        command.StepDefinitionId,
                        command.StepInstanceId,
                        command.Attempt),
                    command.IsCompensation,
                    token),
                ct);
        }
        catch (WorkflowClientException exception) when (exception.StatusCode == StatusCode.NotFound)
        {
            return false;
        }
    }
}