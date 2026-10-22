using System.Globalization;
using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;
using WorkflowScheduler.Contracts.Grpc;
using WorkflowScheduler.Grpc;
using CancelProcessGrpcClient = WorkflowScheduler.Grpc.CancelProcess.CancelProcessClient;
using CompleteStepGrpcClient = WorkflowScheduler.Grpc.CompleteStep.CompleteStepClient;
using GetProcessDefinitionGrpcClient =
    WorkflowScheduler.Grpc.GetProcessDefinition.GetProcessDefinitionClient;
using GetProcessStatusGrpcClient = WorkflowScheduler.Grpc.GetProcessStatus.GetProcessStatusClient;
using IsStepActiveGrpcClient = WorkflowScheduler.Grpc.IsStepActive.IsStepActiveClient;
using RegisterProcessDefinitionGrpcClient =
    WorkflowScheduler.Grpc.RegisterProcessDefinition.RegisterProcessDefinitionClient;
using SendEventGrpcClient = WorkflowScheduler.Grpc.SendEvent.SendEventClient;
using StartProcessGrpcClient = WorkflowScheduler.Grpc.StartProcess.StartProcessClient;

namespace WorkflowScheduler.Client;

public sealed class WorkflowSchedulerClient : IWorkflowSchedulerClient, IDisposable
{
    private readonly CallInvoker _callInvoker;
    private readonly Func<Metadata>? _headers;
    private readonly JsonSerializerOptions _json;
    private readonly GrpcChannel? _ownedChannel;
    private readonly TimeSpan _timeout;
    private bool _disposed;

    public WorkflowSchedulerClient(WorkflowClientOptions options)
    {
        ValidateOptions(options, true);
        _json = CopyJsonOptions(options.JsonSerializerOptions);
        _timeout = options.RequestTimeout;
        _headers = options.HeadersFactory;
        _ownedChannel = options.ChannelOptions is null
            ? GrpcChannel.ForAddress(options.Address!)
            : GrpcChannel.ForAddress(options.Address!, options.ChannelOptions);
        _callInvoker = _ownedChannel.CreateCallInvoker();
    }

    /// <summary>Uses a caller-owned gRPC call invoker, allowing custom interceptors and transport configuration.</summary>
    public WorkflowSchedulerClient(CallInvoker callInvoker, WorkflowClientOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        options ??= new WorkflowClientOptions();
        ValidateOptions(options, false);
        _callInvoker = callInvoker;
        _json = CopyJsonOptions(options.JsonSerializerOptions);
        _timeout = options.RequestTimeout;
        _headers = options.HeadersFactory;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _ownedChannel?.Dispose();
    }

    public Task<RegisteredWorkflow> RegisterAsync(WorkflowDefinition definition,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var request = new RegisterProcessDefinitionRequest { Name = definition.Name, Version = definition.Version };
        foreach (var step in definition.Steps)
            request.Steps.Add(
                new StepDefinitionMessage
                {
                    Id = step.Id.ToString(), Name = step.Name, Metadata = step.Metadata,
                    StepType = step.Kind switch
                    {
                        WorkflowStepKind.Call => StepTypeMessage.Call,
                        WorkflowStepKind.WaitEvent => StepTypeMessage.WaitEvent,
                        WorkflowStepKind.Condition => StepTypeMessage.Condition,
                        _ => throw new ArgumentOutOfRangeException(nameof(definition))
                    },
                    NextStepId = step.NextStepId?.ToString() ?? "",
                    CompensationStepId = step.CompensationStepId?.ToString() ?? ""
                });
        return InvokeAsync(
            nameof(RegisterAsync),
            async options =>
            {
                using var call =
                    new RegisterProcessDefinitionGrpcClient(_callInvoker).RegisterProcessDefinitionAsync(
                        request,
                        options);
                var response = await call.ResponseAsync.ConfigureAwait(false);
                return new RegisteredWorkflow(Guid.Parse(response.DefinitionId), definition.Name, definition.Version);
            },
            cancellationToken);
    }

    public Task<Guid> StartAsync<TInput>(RegisteredWorkflow definition,
        TInput input,
        string businessKey,
        Guid idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return StartAsync(definition.Id, input, businessKey, idempotencyKey, cancellationToken);
    }

    public Task<RegisteredWorkflow> GetDefinitionAsync(string name,
        int version,
        CancellationToken cancellationToken = default)
    {
        Guard.Text(name, nameof(name), 256);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        var request = new GetProcessDefinitionRequest { Name = name, Version = version };
        return InvokeAsync(
            nameof(GetDefinitionAsync),
            async options =>
            {
                using var call =
                    new GetProcessDefinitionGrpcClient(_callInvoker).GetProcessDefinitionAsync(request, options);
                var response = await call.ResponseAsync.ConfigureAwait(false);
                return new RegisteredWorkflow(Guid.Parse(response.DefinitionId), name, version);
            },
            cancellationToken);
    }

    public Task<Guid> StartAsync<TInput>(Guid definitionId,
        TInput input,
        string businessKey,
        Guid idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        Guard.Id(definitionId, nameof(definitionId));
        Guard.Id(idempotencyKey, nameof(idempotencyKey));
        Guard.Text(businessKey, nameof(businessKey), 512);
        var json = JsonSerializer.Serialize(input, _json);
        if (!WorkflowProtocol.InputFitsMessage(json))
            throw new ArgumentException("Workflow input exceeds the command message size limit.", nameof(input));
        using (var document = JsonDocument.Parse(json))
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new ArgumentException("Workflow input must serialize to a JSON object.", nameof(input));
        }

        var request = new StartProcessRequest
        {
            DefinitionId = definitionId.ToString(), InputData = json, BusinessKey = businessKey,
            IdempotencyKey = idempotencyKey.ToString()
        };
        return InvokeAsync(
            nameof(StartAsync),
            async options =>
            {
                using var call = new StartProcessGrpcClient(_callInvoker).StartProcessAsync(request, options);
                return Guid.Parse((await call.ResponseAsync.ConfigureAwait(false)).ProcessInstanceId);
            },
            cancellationToken);
    }

    public Task SendEventAsync<TPayload>(Guid processInstanceId,
        string eventName,
        TPayload payload,
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        Guard.Id(processInstanceId, nameof(processInstanceId));
        Guard.Text(eventName, nameof(eventName), WorkflowProtocol.MaxEventNameLength);
        Guard.Text(correlationId, nameof(correlationId), 256);
        var request = new SendEventRequest
        {
            ProcessInstanceId = processInstanceId.ToString(), EventName = eventName,
            Payload = SerializeOptional(payload), CorrelationId = correlationId
        };
        return InvokeAsync(
            nameof(SendEventAsync),
            async options =>
            {
                using var call = new SendEventGrpcClient(_callInvoker).SendEventAsync(request, options);
                await call.ResponseAsync.ConfigureAwait(false);
                return true;
            },
            cancellationToken);
    }

    public Task CompleteStepAsync<TOutput>(StepExecution execution,
        TOutput output,
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        return CompleteAsync(execution, true, SerializeOptional(output), correlationId, cancellationToken);
    }

    public Task FailStepAsync(StepExecution execution,
        string error,
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        Guard.Text(error, nameof(error));
        return CompleteAsync(execution, false, error, correlationId, cancellationToken);
    }

    public Task CancelAsync(Guid processInstanceId, CancellationToken cancellationToken = default)
    {
        Guard.Id(processInstanceId, nameof(processInstanceId));
        return InvokeAsync(
            nameof(CancelAsync),
            async options =>
            {
                using var call = new CancelProcessGrpcClient(_callInvoker).CancelProcessAsync(
                    new CancelProcessRequest
                        { ProcessInstanceId = processInstanceId.ToString() },
                    options);
                await call.ResponseAsync.ConfigureAwait(false);
                return true;
            },
            cancellationToken);
    }

    public Task<WorkflowProcessSnapshot> GetStatusAsync(Guid processInstanceId,
        CancellationToken cancellationToken = default)
    {
        Guard.Id(processInstanceId, nameof(processInstanceId));
        return GetStatusAsync(
            new GetProcessStatusRequest { ProcessInstanceId = processInstanceId.ToString() },
            nameof(GetStatusAsync),
            cancellationToken);
    }

    public Task<WorkflowProcessSnapshot> GetStatusPageAsync(Guid processInstanceId,
        int offset = 0,
        int pageSize = 100,
        bool includeOutputs = false,
        CancellationToken cancellationToken = default)
    {
        Guard.Id(processInstanceId, nameof(processInstanceId));
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (pageSize is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(pageSize));
        return GetStatusAsync(
            new GetProcessStatusRequest
            {
                ProcessInstanceId = processInstanceId.ToString(), HistoryOffset = offset,
                HistoryPageSize = pageSize, OmitOutputData = !includeOutputs
            },
            nameof(GetStatusPageAsync),
            cancellationToken);
    }

    public Task<bool> IsStepActiveAsync(StepExecution execution,
        bool isCompensation = false,
        CancellationToken cancellationToken = default)
    {
        ValidateExecution(execution);
        var request = new IsStepActiveRequest
        {
            ProcessInstanceId = execution.ProcessInstanceId.ToString(),
            StepDefinitionId = execution.StepDefinitionId.ToString(),
            StepInstanceId = execution.StepInstanceId.ToString(),
            Attempt = execution.Attempt,
            IsCompensation = isCompensation
        };
        return InvokeAsync(
            nameof(IsStepActiveAsync),
            async options =>
            {
                using var call = new IsStepActiveGrpcClient(_callInvoker).IsStepActiveAsync(request, options);
                return (await call.ResponseAsync.ConfigureAwait(false)).Active;
            },
            cancellationToken);
    }

    private Task<WorkflowProcessSnapshot> GetStatusAsync(GetProcessStatusRequest request,
        string operation,
        CancellationToken cancellationToken)
    {
        return InvokeAsync(
            operation,
            async options =>
            {
                using var call = new GetProcessStatusGrpcClient(_callInvoker).GetProcessStatusAsync(request, options);
                var response = await call.ResponseAsync.ConfigureAwait(false);
                var history = response
                    .StepHistory
                    .Select(step => new WorkflowStepHistory(
                        Guid.Parse(step.Id),
                        Guid.Parse(step.StepDefinitionId),
                        Enum.Parse<WorkflowStepStatus>(step.Status),
                        ParseTime(step.StartedAt),
                        string.IsNullOrEmpty(step.CompletedAt) ? null : ParseTime(step.CompletedAt),
                        NullIfEmpty(step.OutputData),
                        NullIfEmpty(step.Error),
                        step.RetryCount))
                    .ToArray();
                return new WorkflowProcessSnapshot(
                    Guid.Parse(response.Id),
                    Guid.Parse(response.DefinitionId),
                    response.BusinessKey,
                    Enum.Parse<WorkflowProcessStatus>(response.Status),
                    string.IsNullOrEmpty(response.CurrentStepId) ? null : Guid.Parse(response.CurrentStepId),
                    ParseTime(response.CreatedAt),
                    ParseTime(response.UpdatedAt),
                    Array.AsReadOnly(history))
                {
                    TotalStepCount = response.TotalStepCount, HistoryOffset = response.HistoryOffset
                };
            },
            cancellationToken);
    }

    internal static void ValidateOptions(WorkflowClientOptions options, bool requiresAddress)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.JsonSerializerOptions);
        if (requiresAddress && (options.Address is not { IsAbsoluteUri: true } address
                                || address.Scheme is not ("http" or "https")))
            throw new ArgumentException("Address must be an absolute HTTP or HTTPS URI.", nameof(options));
        if (options.RequestTimeout != Timeout.InfiniteTimeSpan && options.RequestTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "RequestTimeout must be positive or infinite.");
    }

    private static void ValidateExecution(StepExecution execution)
    {
        ArgumentNullException.ThrowIfNull(execution);
        Guard.Id(execution.ProcessInstanceId, nameof(execution.ProcessInstanceId));
        Guard.Id(execution.StepDefinitionId, nameof(execution.StepDefinitionId));
        Guard.Id(execution.StepInstanceId, nameof(execution.StepInstanceId));
        ArgumentOutOfRangeException.ThrowIfNegative(execution.Attempt);
    }

    private Task CompleteAsync(StepExecution execution,
        bool success,
        string output,
        string correlationId,
        CancellationToken cancellationToken)
    {
        ValidateExecution(execution);
        Guard.Text(correlationId, nameof(correlationId), 256);
        if (!WorkflowProtocol.OutputFitsMessage(output))
            throw new ArgumentException("Step output exceeds the callback message size limit.", nameof(output));
        var request = new CompleteStepRequest
        {
            ProcessInstanceId = execution.ProcessInstanceId.ToString(),
            StepDefinitionId = execution.StepDefinitionId.ToString(),
            StepInstanceId = execution.StepInstanceId.ToString(), Attempt = execution.Attempt,
            Success = success, OutputData = output, CorrelationId = correlationId
        };
        return InvokeAsync(
            success ? nameof(CompleteStepAsync) : nameof(FailStepAsync),
            async options =>
            {
                using var call = new CompleteStepGrpcClient(_callInvoker).CompleteStepAsync(request, options);
                await call.ResponseAsync.ConfigureAwait(false);
                return true;
            },
            cancellationToken);
    }

    private async Task<T> InvokeAsync<T>(string operation, Func<CallOptions, Task<T>> action, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ct.ThrowIfCancellationRequested();
        var options = new CallOptions(
            _headers?.Invoke(),
            cancellationToken: ct,
            deadline: _timeout == Timeout.InfiniteTimeSpan ? null : DateTime.UtcNow.Add(_timeout));
        try
        {
            return await action(options).ConfigureAwait(false);
        }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
        {
            throw new OperationCanceledException("Workflow request was cancelled.", exception, ct);
        }
        catch (RpcException exception)
        {
            throw new WorkflowClientException(operation, exception);
        }
    }

    private string SerializeOptional<T>(T value)
    {
        return value is null ? "" : JsonSerializer.Serialize(value, _json);
    }

    private static string? NullIfEmpty(string value)
    {
        return string.IsNullOrEmpty(value) ? null : value;
    }

    private static DateTimeOffset ParseTime(string value)
    {
        return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }

    private static JsonSerializerOptions CopyJsonOptions(JsonSerializerOptions original)
    {
        var options = new JsonSerializerOptions(original);
        options.MakeReadOnly(true);
        return options;
    }
}