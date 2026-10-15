# WorkflowScheduler.Worker

.NET 10 library hosted inside a domain service. It consumes `process.commands.<ServiceName>` from Kafka, dispatches
typed
commands to your handlers and reports results through `WorkflowScheduler.Client`. Each host reuses one consumer and one
dead-letter producer. Replicas of a service share the same consumer group.

```csharp
using Microsoft.Extensions.Hosting;
using WorkflowScheduler.Client;
using WorkflowScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWorkflowSchedulerClient(options =>
    options.Address = new Uri("https://scheduler.example.com"));
builder.Services.AddWorkflowSchedulerWorker(options =>
{
    options.ServiceName = "billing";
    options.BootstrapServers = "localhost:9092";
})
.Handle<ChargeRequest, ChargeResult, ChargeHandler>("Charge")
.Handle<ChargeRequest, ChargeResult, RefundHandler>("Refund");

await builder.Build().RunAsync();
```

Register the handler's own dependencies using normal DI. `Handle<TInput, TOutput, THandler>` registers the handler as
scoped
unless you already registered it. Each message gets a new asynchronous DI scope, disposed after the handler finishes.
The command name matches the name in `.Call("billing", "Charge")` or `.CompensateWith("billing", "Refund")` exactly.

```csharp
public sealed record ChargeRequest(int OrderId, decimal Amount);
public sealed record ChargeResult(string PaymentId);

public sealed class ChargeHandler(IPayments payments)
    : IWorkflowCommandHandler<ChargeRequest, ChargeResult>
{
    public async Task<ChargeResult> HandleAsync(
        ChargeRequest input, WorkflowCommandContext context, CancellationToken cancellationToken)
    {
        // IPayments is the domain service's dependency. Its implementation persists
        // and reuses the result for this execution key before repeating a charge.
        return await payments.ChargeAsync(input.OrderId, input.Amount,
            context.IdempotencyKey, cancellationToken);
    }
}
```

`RefundHandler` implements the same interface for a refund. Compensation is an ordinary registered command;
`context.IsCompensation` identifies it. `context.Execution` contains process, definition-step and step-execution IDs
plus
the attempt number. `context.CorrelationId` is the original command correlation. `context.IdempotencyKey` combines the
execution ID and attempt, while `context.CallbackCorrelationId` is stable across transport retries and changes for a new
scheduled attempt. A retried step retains its execution ID, so using only that ID as the callback correlation would
collide with an earlier failure callback.

For a stateless handler, a delegate is also available:

```csharp
worker.Handle<ValidateRequest, ValidateResult>("Validate", (input, context, ct) =>
    Task.FromResult(new ValidateResult(input.OrderId > 0)));
```

Use the class overload for scoped dependencies. Both overloads deserialize the original process input with web/camelCase
JSON defaults and serialize the typed output. A handler exception, invalid typed input or an unregistered command sends
a
failure callback. The scheduler owns delayed retries and compensation; the worker executes each published attempt once
per delivery. Outputs are recorded in history; the scheduler continues to forward the original input to later commands.

## Delivery and shutdown

The worker checks persisted process/step state before executing a command. It skips completed, cancelled, missing and
superseded executions. Running compensation steps are allowed while the process remains `Failed` or `Cancelled`.
Transport retries keep the already computed output or error in memory and resend the same callback identity. A callback
conflict is acknowledged only after checking that the execution is no longer active. An active execution's conflict is
retried and eventually fails the worker; it is never silently acknowledged.

Kafka offset storage is disabled until a callback is accepted or an inactive execution is confirmed. Stored offsets are
committed by librdkafka in batches, including on a clean close. This follows the client's
[manual offset storage pattern](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#store-offsets).
The handler runs sequentially in each worker, preserving partition order. More topic partitions and service replicas
provide concurrency. The consumer, producer and gRPC channel are reused throughout the host lifetime.

Delivery is at least once. A restart after a business side effect and before its callback can execute the handler again.
Persist/deduplicate the operation and its result in the domain service using `context.IdempotencyKey`; the package does
not impose a database or claim exactly-once business effects. Keep a business-level idempotency key too when an
operation
must stay unique across distinct workflow retry attempts. Cancellation may race a handler already doing external work.

Malformed envelopes, invalid execution identities, mismatched Kafka process keys and tombstones go to
`process.commands.<ServiceName>.dead-letter`. `WorkflowDeadLetter` preserves the source topic, partition, offset, key,
payload and parse error; its Kafka key is `<source-topic>:<partition>:<offset>`. The source offset is stored only after
the
broker acknowledges the dead letter. Valid commands with no registered handler report failure to the scheduler.
Provision both topics with the required partitions, replication and ACLs before running the worker.

Shutdown cancels the current handler/RPC, waits for it to finish and closes the consumer. An unfinished record remains
unacknowledged. Handlers must honor their cancellation token. Exhausted transient retries, authentication errors and
other unexpected infrastructure failures fault the background service. Generic Host's default behavior stops the host;
its supervisor can restart it and Kafka redelivers uncommitted records. The package does not retry business operations
locally after they throw.

## Configuration

| Option                     | Default                                                         | Purpose                                                |
|----------------------------|-----------------------------------------------------------------|--------------------------------------------------------|
| `ServiceName`              | Required                                                        | Command topic suffix; must produce a valid Kafka topic |
| `BootstrapServers`         | Required                                                        | Kafka brokers                                          |
| `GroupId`                  | `workflow-<ServiceName>`                                        | Stable group shared by replicas                        |
| `DeadLetterTopic`          | `<command-topic>.dead-letter`                                   | Invalid message destination                            |
| `MaxDeliveryAttempts`      | 5                                                               | Attempts for one status/callback/dead-letter operation |
| `RetryDelay`               | 1 second                                                        | Delay between transient transport failures             |
| `RpcTimeout`               | 10 seconds                                                      | Cancellation deadline for each transport attempt       |
| `HandlerTimeout`           | 1 minute                                                        | Cooperative handler cancellation deadline              |
| `JsonSerializerOptions`    | Web defaults                                                    | Typed input/output serialization                       |
| `ConsumerConfig`           | Earliest; 1-second commit interval; 15-minute max poll interval | Kafka fetch/group/security settings                    |
| `DeadLetterProducerConfig` | Idempotent; all replicas acknowledge                            | Dead-letter transport/security settings                |

The worker copies configuration at registration. It enforces automatic batched commits with manual offset storage and
an idempotent dead-letter producer. It validates that `MaxPollIntervalMs` exceeds the configured handler and
conservative
RPC/reconciliation retry budget. Increase it when increasing handler timeouts. A handler that ignores cancellation can
still exceed that budget. Configure SASL/TLS on **both** Kafka configuration objects; gRPC security belongs in
`WorkflowClientOptions`. Do not change a replica's group ID on every deployment.

## Local packages

```bash
dotnet pack src/WorkflowScheduler.Contracts.Grpc --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Client --configuration Release --output artifacts/packages
dotnet pack src/WorkflowScheduler.Worker --configuration Release --output artifacts/packages
```

Add `artifacts/packages` to your NuGet sources and install `WorkflowScheduler.Worker` version `0.1.0`. The
`WorkflowScheduler.Client` and `WorkflowScheduler.Contracts.Grpc` packages must be available in the feed. The Worker
package does not depend on the scheduler application.
