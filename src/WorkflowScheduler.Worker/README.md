# WorkflowScheduler.Worker

Use this .NET 10 SDK inside the service that executes workflow commands. The Worker receives a command from Kafka,
calls your handler and reports success or failure to the scheduler through the Client SDK.

The scheduler controls step order and delayed business retries. Your service owns the operation and its durable result.

## Install

Build the packages from the repository root:

```bash
dotnet pack src/WorkflowScheduler.Contracts --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Client --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Worker --configuration Release --output artifacts/packages
PACKAGE_FEED="$(pwd)/artifacts/packages"
dotnet nuget add source "$PACKAGE_FEED" --name workflow-local
```

Add this source once; skip `dotnet nuget add source` if the Client setup already added it. Keep nuget.org configured for
public dependencies. Then create a Worker host, or use your existing service project:

```bash
dotnet new worker --name Example.Worker --framework net10.0
dotnet add Example.Worker/Example.Worker.csproj package WorkflowScheduler.Worker --version 0.1.0
```

The Worker package brings in the Client and Contracts packages as dependencies.

## Run your first handler

Start the [local scheduler](../../README.md#run-the-scheduler-locally). From the repository root, create the command and
dead-letter topics in its local Kafka broker:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic process.commands.inventory --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic process.commands.inventory.dead-letter --partitions 1 --replication-factor 1
```

Replace `Example.Worker/Program.cs` with:

```csharp
using Microsoft.Extensions.Hosting;
using WorkflowScheduler.Client;
using WorkflowScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowSchedulerClient(options =>
    options.Address = new Uri("http://localhost:5125"));
builder.Services.AddWorkflowSchedulerWorker(options =>
{
    options.ServiceName = "inventory";
    options.BootstrapServers = "localhost:9092";
})
.Handle<OrderInput, ReservationResult>("ReserveStock", (input, context, ct) =>
{
    Console.WriteLine($"ReserveStock for order {input.OrderId}, attempt {context.Execution.Attempt}");
    return Task.FromResult(new ReservationResult($"demo-{input.OrderId}"));
});

await builder.Build().RunAsync();

public sealed record OrderInput(int OrderId);
public sealed record ReservationResult(string ReservationId);
```

Run `dotnet run --project Example.Worker` and keep it running. Then execute the
[Client example](../WorkflowScheduler.Client/README.md#start-your-first-process).
The Worker prints the order ID and the Client observes `Completed`. This handler only returns a demonstration result;
see the next section before implementing a business side effect.

`ServiceName = "inventory"` selects `process.commands.inventory`. `ReserveStock` must exactly match the command in
`.Call("inventory", "ReserveStock")`. Input and output use web/camelCase JSON defaults.
You do not need to write a Kafka consumer or call `CompleteStepAsync` in the handler.

## Implement a business operation

Use the class overload when the handler needs scoped dependencies:

```csharp
worker.Handle<OrderInput, ReservationResult, ReserveStockHandler>("ReserveStock");
```

Here `worker` is the builder returned by `AddWorkflowSchedulerWorker`. Your `ReserveStockHandler` implements
`IWorkflowCommandHandler<OrderInput, ReservationResult>` and its `HandleAsync` method receives the input, a
`WorkflowCommandContext` and a cancellation token. Register its dependencies through normal DI. The SDK registers the
handler as scoped unless it is already registered; each message gets a fresh asynchronous scope.

For a durable implementation, follow the
demo's [Inventory handler](../../samples/order-demo/OrderDemo.Inventory/InventoryHandler.cs)
or [Payment handler](../../samples/order-demo/OrderDemo.Payments/PaymentHandler.cs). Commit the business change and its
execution receipt together, and return the stored result when the receipt already exists.

| Context property        | Use                                                                              |
|-------------------------|----------------------------------------------------------------------------------|
| `IdempotencyKey`        | Deduplicate this execution and attempt; persist it with the result               |
| `Execution`             | Inspect process, definition-step and execution IDs plus the attempt number       |
| `Command`               | Distinguish commands when one handler handles several operations                 |
| `IsCompensation`        | Identify an undo command                                                         |
| `CallbackCorrelationId` | Stable callback identity managed by the SDK; changes for a new scheduled attempt |

Delivery is **at least once**. If the service crashes after committing a business operation but before the scheduler
accepts its callback, the handler can run again. A business-level key, such as a payment transaction ID, is also needed
when an operation must remain unique across different workflow retry attempts.

Compensation is another registered command, such as `ReleaseStock`. Register it with `Handle` just like a forward
command. The scheduler chooses when to execute it; the handler performs the domain-specific undo operation.

## Results, retries and shutdown

- Returning a result sends a success callback. Throwing an exception, invalid typed input or a missing handler sends a
  failure callback. The scheduler then applies its retry and compensation policy.
- A serialized output over 3 MiB produces a small failure callback. Store large results externally and return a
  reference.
- Callback transport failures retry the already computed result in memory. The SDK does not rerun the handler as a local
  transport retry. A process restart can still cause redelivery.
- Before execution, the Worker checks that the execution and attempt are active. It skips completed, cancelled and stale
  commands. Running compensation remains valid while the process is `Failed` or `Cancelled`.
- The Kafka offset becomes eligible for commit only after callback acknowledgement or confirmation that the execution is
  inactive. Handler cancellation during shutdown leaves unfinished delivery unacknowledged.
- Honour the handler's cancellation token. Exhausted transport retries or unexpected infrastructure failures fault the
  Worker; Generic Host normally stops the host so its supervisor can restart it.

Each Worker executes handlers sequentially. More command-topic partitions and service replicas provide concurrency;
replicas of the same service must share a stable consumer group. Cancellation can race a business operation already
in progress.

## Configuration

Configure these options inside `AddWorkflowSchedulerWorker` before building the host:

| Option                     | Default                                                         | Purpose                                                           |
|----------------------------|-----------------------------------------------------------------|-------------------------------------------------------------------|
| `ServiceName`              | Required                                                        | Command topic suffix                                              |
| `BootstrapServers`         | Required                                                        | Kafka brokers                                                     |
| `GroupId`                  | `workflow-<ServiceName>`                                        | Stable group shared by service replicas                           |
| `DeadLetterTopic`          | `<command-topic>.dead-letter`                                   | Destination for invalid envelopes                                 |
| `HandlerTimeout`           | 1 minute                                                        | Cooperative handler cancellation deadline                         |
| `RpcTimeout`               | 10 seconds                                                      | Timeout of each status, callback or dead-letter transport attempt |
| `MaxDeliveryAttempts`      | 5                                                               | Attempts for one transport operation                              |
| `RetryDelay`               | 1 second                                                        | Delay between transient transport failures                        |
| `JsonSerializerOptions`    | Web defaults                                                    | Typed payload serialization                                       |
| `ConsumerConfig`           | Earliest; 1-second commit interval; 15-minute max poll interval | Kafka consumer and security settings                              |
| `DeadLetterProducerConfig` | Idempotent; all replicas acknowledge                            | Kafka producer and security settings                              |

These delivery attempts are separate from the scheduler's delayed business retries. Increasing `HandlerTimeout` or
transport retry settings may require a larger `ConsumerConfig.MaxPollIntervalMs`; registration validates that budget.
A handler that ignores cancellation can still overrun it.

For a secured broker, configure SASL/TLS on **both** `ConsumerConfig` and `DeadLetterProducerConfig`. Configure
scheduler
gRPC transport through `WorkflowClientOptions`. Provision command and dead-letter topics with the required partitions,
replication and permissions before starting the service. Keep the same group ID across deployments.

<details>
<summary>Invalid messages and delivery details</summary>

Malformed envelopes, invalid execution identities, mismatched Kafka process keys and tombstones go to the dead-letter
topic. A valid command with no registered handler instead reports failure to the scheduler.

`WorkflowDeadLetter` records the source topic, partition, offset, key, payload and error. Its Kafka key is
`<source-topic>:<partition>:<offset>`. The source offset is stored only after the broker acknowledges the dead letter.
Oversized payloads retain a prefix, `PayloadTruncated`, `OriginalPayloadBytes` and `PayloadSha256`; shortened keys set
`KeyTruncated`. `DeadLetterProducerConfig.MessageMaxBytes` must be at least 8192 bytes, and broker/topic limits must
permit that size.

The Worker uses automatic batched commits with manual offset storage. A callback conflict is acknowledged only after
`IsStepActive` confirms the execution is inactive. Active conflicts are retried and can fault the Worker; they are not
silently treated as success. Deploy a scheduler supporting `IsStepActive` before updating Workers that require it.

</details>
