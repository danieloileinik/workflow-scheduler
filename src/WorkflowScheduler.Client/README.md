# WorkflowScheduler.Client

.NET 10 client library for the existing Workflow Scheduler gRPC API. It builds workflow definitions, serializes typed
payloads, sets RPC deadlines and maps gRPC failures to `WorkflowClientException`. It connects directly to the scheduler.

```csharp
using WorkflowScheduler.Client;

services.AddWorkflowSchedulerClient(options =>
{
    options.Address = new Uri("https://scheduler.example.com");
    options.RequestTimeout = TimeSpan.FromSeconds(30);
});
```

Inject `IWorkflowSchedulerClient`. A singleton reuses its gRPC channel, which the service provider disposes. For
standalone
use, create and dispose `new WorkflowSchedulerClient(new WorkflowClientOptions { Address = address })`.
You can also pass a caller-owned `Grpc.Core.CallInvoker`, for example `channel.CreateCallInvoker()`
or an invoker with interceptors; its channel remains caller-owned.

```csharp
var definition = Workflow.Define("Order", version: 1)
    .Call("inventory", "ReserveStock")
    .CompensateWith("inventory", "ReleaseStock")
    .WaitFor("OrderApproved")
    .Condition("amount", WorkflowComparison.GreaterThanOrEqual, 100m,
        whenTrue: branch => branch.Call("payments", "ChargeLargeOrder"),
        whenFalse: branch => branch.Call("payments", "ChargeSmallOrder"))
    .Call("shipping", "ShipOrder")
    .Build();

var registered = await scheduler.RegisterAsync(definition, cancellationToken);
var processId = await scheduler.StartAsync(registered, new { orderId = 42, amount = 150m },
    businessKey: "order-42", idempotencyKey: requestId, cancellationToken: cancellationToken);
await scheduler.SendEventAsync(processId, "OrderApproved", new { approvedBy = "manager" },
    correlationId: "approval-42", cancellationToken: cancellationToken);
var status = await scheduler.GetStatusAsync(processId, cancellationToken);
```

Both condition branches must contain a step. Steps appended after a condition are shared continuations. Conditions use
top-level input fields and the server's `eq`, `ne`, `gt`, `lt`, `gte`, `lte` operators. Numeric values use invariant
culture.
Equality follows the server's ordinal text comparison; numeric ordering uses decimal comparison.
`CompensateWith` attaches a separate terminal Call to the preceding Call. The builder produces immutable snapshots and
stable step IDs for the same definition name, version and construction sequence. Changing a definition requires a new
version. Loops and arbitrary graph editing are not exposed by this first builder.

Registration is not an upsert: the server rejects an existing name/version. Persist the returned definition ID and reuse
it for subsequent starts. If a registration response is lost, recover the ID with
`GetDefinitionAsync(name, version, cancellationToken)`; missing definitions return `NotFound`.
Supply and persist the same idempotency
key
when retrying a start and the same correlation ID when retrying an event/callback. The SDK does not automatically retry
mutating RPCs or generate new request identities. Input is serialized as a JSON object using web/camelCase defaults;
customize `JsonSerializerOptions` before creating the client. Event/output payloads may be null.

`CancelAsync` cancels a process. Call results can be submitted using
`CompleteStepAsync(new StepExecution(processId, definitionStepId, executionId, attempt), output, correlationId)` or
`FailStepAsync(execution, error, correlationId)`. `WorkflowStepHistory.Execution(processId)` creates that context from a
status snapshot, and `ReadOutput<T>()` reads a JSON output. The package does not consume Kafka or execute domain
commands.

For growing histories, use `GetStatusPageAsync(processId, offset: 0, pageSize: 100)` (page size 1..100).
The snapshot includes `TotalStepCount` and `HistoryOffset`; outputs are omitted by default. Request individual output
pages with `includeOutputs: true` and a suitable page size. The original `GetStatusAsync` retains its full-history
behavior for compatibility; full or output-heavy responses may require a larger gRPC receive limit in `ChannelOptions`.

`WorkflowClientException` preserves `StatusCode`, `Detail`, trailers and the original `RpcException`. Caller
cancellation
throws `OperationCanceledException`; deadline expiration remains a `DeadlineExceeded` client exception.
`HeadersFactory` can supply fresh authorization metadata per RPC; `ChannelOptions` configures TLS/HTTP transport.
The scheduler forwards original input to calls and does not automatically merge step outputs into process input.

Build the local NuGet feed from the repository root:

```bash
dotnet pack src/WorkflowScheduler.Contracts --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Client --configuration Release --output artifacts/packages
```

The Client package depends on the existing `WorkflowScheduler.Contracts` package; both must be available in the
feed.
Add `artifacts/packages` as a NuGet source alongside nuget.org, then install `WorkflowScheduler.Client` version `0.1.0`.
Public dependencies (gRPC, protobuf and DI abstractions) are restored from your normal NuGet sources.
