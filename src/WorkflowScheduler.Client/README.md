# WorkflowScheduler.Client

Use this .NET 10 SDK in the application that starts workflows. It connects to the scheduler over gRPC.
Command execution belongs in a separate [Worker](../WorkflowScheduler.Worker/README.md).

## Install

Build the local packages from the repository root:

```bash
dotnet pack src/WorkflowScheduler.Contracts --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Client --configuration Release --output artifacts/packages
PACKAGE_FEED="$(pwd)/artifacts/packages"
dotnet nuget add source "$PACKAGE_FEED" --name workflow-local
```

Add this source once. It must remain available alongside nuget.org so that both local SDK packages and their public
dependencies can be restored.

For a standalone example, create a console application:

```bash
dotnet new console --name Example.Client --framework net10.0
dotnet add Example.Client/Example.Client.csproj package WorkflowScheduler.Client --version 0.1.0
```

For an existing application, use its project path instead.

## Start your first process

Start the [local scheduler](../../README.md#run-the-scheduler-locally) and the
[inventory Worker example](../WorkflowScheduler.Worker/README.md#run-your-first-handler) first.
Replace `Example.Client/Program.cs` with:

```csharp
using Grpc.Core;
using WorkflowScheduler.Client;

using var scheduler = new WorkflowSchedulerClient(new WorkflowClientOptions
{
    Address = new Uri("http://localhost:5125")
});

var definition = Workflow.Define("reserve-stock", version: 1)
    .Call("inventory", "ReserveStock")
    .Build();

RegisteredWorkflow registered;
try
{
    registered = await scheduler.GetDefinitionAsync(definition.Name, definition.Version);
}
catch (WorkflowClientException exception) when (exception.StatusCode == StatusCode.NotFound)
{
    registered = await scheduler.RegisterAsync(definition);
}

var requestId = Guid.NewGuid();
var processId = await scheduler.StartAsync(registered, new { orderId = 42 },
    businessKey: "order-42", idempotencyKey: requestId);
Console.WriteLine($"Started process: {processId}");

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
while (true)
{
    var status = await scheduler.GetStatusPageAsync(processId, cancellationToken: timeout.Token);
    if (status.Status is WorkflowProcessStatus.Completed or WorkflowProcessStatus.Failed or WorkflowProcessStatus.Cancelled)
    {
        Console.WriteLine($"Result: {status.Status}");
        break;
    }

    await Task.Delay(500, timeout.Token);
}
```

Run `dotnet run --project Example.Client`. With the matching Worker running, the result is `Completed`.
The start call returns before the handler finishes; the loop is a bounded demonstration of status polling.

Each run above starts a new process. In an application, persist `requestId` with the incoming business request before
calling the scheduler and restore the same key when retrying. Reusing a key with different definition, business key or
serialized input is rejected. A business key labels a process; it does not deduplicate starts by itself.

Definitions are immutable. Register each name/version once, save its ID and reuse it. Change the version when changing
its steps. If another application registers the same version concurrently, registration can return `FailedPrecondition`;
recover the ID with `GetDefinitionAsync`. If a registration response is lost, use the same lookup.

## Use dependency injection

Inside an existing host, register the Client once and inject `IWorkflowSchedulerClient`:

```csharp
using WorkflowScheduler.Client;

builder.Services.AddWorkflowSchedulerClient(options =>
{
    options.Address = new Uri("http://localhost:5125");
    options.RequestTimeout = TimeSpan.FromSeconds(30);
});
```

The registration reuses a singleton gRPC channel and disposes it with the host. A standalone client, as above, should be
disposed by its caller. Configure `JsonSerializerOptions`, `ChannelOptions` and a per-RPC `HeadersFactory` before
creating
the client when you need custom JSON, TLS, transport limits or authorization metadata.

## Add waiting steps and compensation

This definition follows the [order demo](../../samples/order-demo/README.md):

```csharp
var orderDefinition = Workflow.Define("approved-order", version: 1)
    .Call("inventory", "ReserveStock")
    .CompensateWith("inventory", "ReleaseStock")
    .Call("payments", "ChargePayment")
    .CompensateWith("payments", "RefundPayment")
    .WaitFor("OrderApproved")
    .Call("inventory", "ShipOrder")
    .Build();
```

`CompensateWith` attaches an undo call to the preceding call. Successful calls are compensated in reverse execution
order after cancellation or exhausted retries. Register matching handlers for forward and compensation commands.

After registering and starting this definition, save its process ID as `orderProcessId`. An approval request can check
whether it has reached the waiting step before sending the event:

```csharp
var status = await scheduler.GetStatusAsync(orderProcessId);
var current = status.StepHistory.SingleOrDefault(step => step.Id == status.CurrentStepId);
var approvalStepId = orderDefinition.Steps.Single(step => step.Kind == WorkflowStepKind.WaitEvent).Id;

if (current is null || current.StepDefinitionId != approvalStepId || current.Status != WorkflowStepStatus.Waiting)
    throw new InvalidOperationException("The order is not ready for approval. Check its status before retrying.");

await scheduler.SendEventAsync(orderProcessId, "OrderApproved", new { approvedBy = "operator-1" },
    correlationId: $"approval:{orderProcessId:N}");
```

Do not send the event immediately after starting a workflow that has earlier calls. There is no early-event buffer.
Process status `Waiting` alone is insufficient: a running call also puts the process in that state.
A state change can race the check, so handle a rejected event normally. Reuse the correlation ID for retries of the same
logical event; an already accepted event returns `FailedPrecondition` without advancing the workflow again.

Use `await scheduler.CancelAsync(orderProcessId)` to request cancellation. It does not stop an operation already
executing in another service; compensation may continue after the process status becomes `Cancelled`.

## Add a condition

```csharp
var paymentDefinition = Workflow.Define("choose-payment", version: 1)
    .Condition("amount", WorkflowComparison.GreaterThanOrEqual, 100m,
        whenTrue: branch => branch.Call("payments", "ChargeLargeOrder"),
        whenFalse: branch => branch.Call("payments", "ChargeSmallOrder"))
    .Build();
```

Both branches need at least one step. Steps appended after a condition are shared continuations. Conditions read
original top-level input fields; numeric ordering uses invariant decimal comparison, while equality compares text
exactly. Outputs from earlier calls are not merged into input. The builder supports sequences and branches, but not
arbitrary graph editing or loops. It produces stable step IDs for the same definition name, version and construction
sequence.

## Read history and handle errors

Prefer bounded history pages for long workflows:

```csharp
var page = await scheduler.GetStatusPageAsync(processId, offset: 0, pageSize: 100, includeOutputs: true);
var firstOutput = page.StepHistory.FirstOrDefault()?.ReadOutput<ReservationResult>();
```

Declare the matching output type in your application:

```csharp
public sealed record ReservationResult(string ReservationId);
```

Pages allow 1..100 entries and omit outputs by default. Use `TotalStepCount` and `HistoryOffset` to request subsequent
pages. `GetStatusAsync` returns the full history; large histories or outputs can require a higher gRPC receive limit in
`ChannelOptions`. Input must serialize to a JSON object; callback output is limited to 3 MiB of UTF-8 data.

`WorkflowClientException` preserves the gRPC `StatusCode`, detail, trailers and original exception. Common cases:

| Status                             | What to do                                                                    |
|------------------------------------|-------------------------------------------------------------------------------|
| `NotFound`                         | Check the definition/process ID; register a missing definition if appropriate |
| `InvalidArgument`                  | Correct the request or payload                                                |
| `FailedPrecondition`               | Check current state or a reused identity; do not blindly repeat the mutation  |
| `Unavailable` / `DeadlineExceeded` | The outcome may be unknown; retry with the saved request identity             |

Caller cancellation throws `OperationCanceledException`. The Client does not automatically retry mutating RPCs.

<details>
<summary>Custom transports and callbacks without the Worker SDK</summary>

`new WorkflowSchedulerClient(callInvoker, options)` accepts a caller-owned `Grpc.Core.CallInvoker`, for example from a
channel with interceptors. Disposing the Client does not dispose that channel.

A custom command consumer can call `CompleteStepAsync` or `FailStepAsync` with a `StepExecution` containing the exact
process, definition-step, execution IDs and attempt. `WorkflowStepHistory.Execution(processId)` creates that context
from history. `IsStepActiveAsync` checks whether that exact execution is still active without loading history.
See the [direct integration protocol](../../README.md#reference) for command identities and acknowledgement rules.
The Worker SDK already handles this protocol for .NET command handlers.

</details>
