# Workflow Scheduler

A domain-independent business-process orchestrator built with .NET 10, gRPC, PostgreSQL, Redis, and Kafka.

Workflow Scheduler coordinates services that own different business domains. For example, an order workflow can ask an
inventory service to reserve stock, wait for an approval event, and ask a payment service to charge the customer.
Neither domain service needs to call or understand the other service. The scheduler owns the process definition,
execution history, retries, and compensation order; domain services own their business operations.

The project currently supports **sequential workflows with conditional branches and loops through waiting steps**. It
does not execute arbitrary scripts, run parallel branches, or provide a visual workflow designer.

## Contents

- [Architecture](#architecture)
- [Running locally](#running-locally)
- [Runnable sample](#runnable-sample)
- [Process definitions and metadata](#process-definitions-and-metadata)
- [gRPC API](#grpc-api)
- [Client SDK](#client-sdk)
- [Worker SDK](#worker-sdk)
- [Domain-service integration](#domain-service-integration)
- [Reliability and consistency](#reliability-and-consistency)
- [Testing](#testing)
- [Limitations](#limitations)

## Architecture

| Component                           | Responsibility                                                                                             |
|-------------------------------------|------------------------------------------------------------------------------------------------------------|
| `Domain/Processes`                  | Definition versions, process state, event deduplication, durable start receipts, saga compensation records |
| `Domain/Steps`                      | Step definitions, execution attempts, results, and history                                                 |
| `Features/Processes/Commands`       | Registration, start, callbacks, external events, cancellation, due retries                                 |
| `Features/Processes/Queries`        | Process status and ordered execution history                                                               |
| `Features/Processes/StepStrategies` | `Call`, `WaitEvent`, `Condition`, iterative execution, and saga coordination                               |
| `Features/Processes/Endpoints`      | Six gRPC methods and protocol/domain error mapping                                                         |
| `Common/Behaviors`                  | Validation and command transaction boundaries                                                              |
| `Infrastructure/Persistence`        | EF Core mappings and PostgreSQL persistence                                                                |
| `Infrastructure/Caching`            | Optional Redis cache for immutable definitions                                                             |
| `Infrastructure/Messaging`          | Kafka publication and durable outbox/retry workers                                                         |
| `WorkflowScheduler.Contracts.Grpc`  | gRPC protocol types and the shared Kafka command envelope                                                  |
| `WorkflowScheduler.Client`          | Fluent workflow definitions, typed payloads/statuses and a reusable gRPC client                            |
| `WorkflowScheduler.Worker`          | Typed Kafka command handlers, scoped execution, retries for callbacks and safe offset acknowledgement      |

A command passes through validation and the transaction behavior before its handler runs. Successful commands persist
their state changes, event receipt, and outbox records in one transaction. Rejected commands roll back. Read queries do
not save changes.

This is a feature-oriented implementation in one application assembly, with separate contract projects. The domain
models do not call external services. Handlers currently use EF Core directly; the repository does not claim a separate
Application/Infrastructure assembly boundary.

## Running locally

### Requirements

- .NET 10 SDK.
- Docker with Docker Compose for local PostgreSQL, Redis, and Kafka.
- Optional: [`grpcurl`](https://github.com/fullstorydev/grpcurl) for command-line API calls.

Run from the repository root:

```bash
docker compose up -d --wait
dotnet restore WorkflowScheduler.slnx
dotnet build WorkflowScheduler.slnx

# Initialize an EMPTY development database once.
dotnet run --project src/WorkflowScheduler --launch-profile http -- --initialize-database

# Start the gRPC server and background workers.
dotnet run --project src/WorkflowScheduler --launch-profile http
```

The development gRPC endpoint is `http://localhost:5125`, using HTTP/2 without TLS. The initialization command creates
tables and exits; it does not start the server or workers. `EnsureCreated` is intended for a fresh development database
and does **not** migrate an existing schema. No database is deleted by these commands.

`docker compose down` stops the dependencies. The PostgreSQL named volume retains data. The supplied broker/cache
configuration and credentials are for local development.

### Configuration

Development defaults live in `src/WorkflowScheduler/appsettings.Development.json`.

| Setting                            | Development value                                                                | Purpose                     |
|------------------------------------|----------------------------------------------------------------------------------|-----------------------------|
| `ConnectionStrings:Postgres`       | `Host=localhost;Port=5432;Database=workflow;Username=workflow;Password=workflow` | Durable application state   |
| `ConnectionStrings:Redis`          | `localhost:6379,abortConnect=false`                                              | Definition cache            |
| `Kafka:BootstrapServers`           | `localhost:9092`                                                                 | Command broker              |
| `Kestrel:Endpoints:Grpc:Url`       | `http://localhost:5125`                                                          | gRPC listener               |
| `Kestrel:Endpoints:Grpc:Protocols` | `Http2`                                                                          | Required for plaintext gRPC |

Environment variables override configuration; use `__` for nesting:

```bash
export ConnectionStrings__Postgres='Host=db;Database=workflow;Username=workflow;Password=<password>'
export ConnectionStrings__Redis='cache:6379,abortConnect=false'
export Kafka__BootstrapServers='broker:9092'
export Kestrel__Endpoints__Grpc__Url='http://0.0.0.0:5125'
export Kestrel__Endpoints__Grpc__Protocols='Http2'
dotnet run --project src/WorkflowScheduler --no-launch-profile
```

Outside development, provide these settings explicitly. Configure TLS/authentication at the deployment boundary before
exposing the service to untrusted clients.

## Runnable sample

The [order demo README](samples/order-demo/README.md) explains a runnable system with Order, Inventory and Payment
services: the internal SDK calls, Kafka commands, gRPC callbacks, HTTP examples, retries and compensation.

## Process definitions and metadata

A definition has a name, a positive version, and an ordered list of steps. `(name, version)` is unique. Definitions are
immutable through the API; register a new version to change a workflow. Step UUIDs must be globally unique, including
across definition versions.

The **first supplied step is the entry step**. Its position is persisted and survives database reloads and cache round
trips. Subsequent steps are selected by references, not by list order. All references must point to steps in the same
definition. Registration rejects duplicate IDs, unknown step types, malformed metadata, missing references, forward
references to compensation steps, and cycles made entirely of automatic conditions. Loops that pass through a `Call` or
`WaitEvent` are valid; each traversal creates a new step-instance ID.

Metadata property names are case-insensitive. The following examples use camelCase. Metadata is a JSON object encoded as
a string inside a protobuf request.

Duplicate metadata property names, including casing variants, are rejected. Event names are limited to 250 characters,
commands to 256, and service names must form a valid Kafka command topic. Input is limited to 980,000 UTF-8 bytes after
JSON string escaping, leaving room for the envelope below the producer's 1,000,000-byte record limit.

### Call

```json
{
  "serviceName": "inventory",
  "command": "ReserveStock"
}
```

Enqueues a command to `process.commands.inventory` and waits for `CompleteStep`. The process becomes `Waiting`; the call
step stays `Running` until its current attempt receives a valid callback.

### WaitEvent

```json
{
  "eventName": "OrderApproved"
}
```

Waits for `SendEvent` with the specified name. Event-name matching and deduplication are case-insensitive. Both the
process and the step are `Waiting`. `CompleteStep` cannot complete this step type.

### Condition

```json
{
  "inputKey": "amount",
  "operator": "gte",
  "value": "100.50",
  "trueNextStepId": "33333333-3333-4333-8333-333333333333",
  "falseNextStepId": "44444444-4444-4444-8444-444444444444"
}
```

Evaluates a top-level property of the original process input and immediately chooses a branch. Operators are `eq`, `ne`,
`gt`, `lt`, `gte`, and `lte`. Numeric comparisons use invariant-culture decimal parsing; a decimal point works under
Russian and other system locales. A missing property or non-numeric input in a numeric comparison selects the false
branch. Equality compares textual values exactly and is case-sensitive. Both branch IDs are required; represent a
branch's end with a terminal step.

The executor advances through automatic steps iteratively, avoiding recursive stack growth. It stops when it reaches a
waiting step or the end of the workflow. A runtime cycle guard also protects against corrupt definitions that bypassed
API validation.

### Compensation

A forward `Call` can set `compensationStepId` to a separate terminal `Call` in the same definition. The compensation
step must have neither a next-step reference nor its own compensation reference. Compensation steps cannot be part of
the forward path.

For example, `ReserveStock` can reference `ReleaseStock`, and `ChargePayment` can reference `RefundPayment`. On
cancellation or exhausted forward-step retries, the scheduler compensates **successfully completed** calls in reverse
execution order. It starts the next compensation only after the previous compensation succeeds. Each compensation has
its own step-instance ID, callback, retry count, and durable log. A successful compensation marks the original step
`Compensated`.

The process keeps its terminal status (`Failed` or `Cancelled`) while compensations run. If a compensation exhausts its
retries, older compensations remain pending and the chain stops for intervention. No automatic manual-repair API is
provided.

## gRPC API

Contract: [
`src/WorkflowScheduler.Contracts.Grpc/processes.proto`](src/WorkflowScheduler.Contracts.Grpc/processes.proto). Service
name: `workflow.Processes`.

Server reflection is not enabled. Supply the proto explicitly:

```bash
grpcurl -plaintext \
  -import-path src/WorkflowScheduler.Contracts.Grpc \
  -proto processes.proto \
  -d '<request JSON>' \
  localhost:5125 workflow.Processes.<Method>
```

### RegisterProcessDefinition

Example: reserve inventory, wait for approval, charge payment; each completed call has an undo action.

```json
{
  "name": "approved-order",
  "version": 1,
  "steps": [
    {
      "id": "11111111-1111-4111-8111-111111111111",
      "name": "Reserve inventory",
      "stepType": "CALL",
      "metadata": "{\"serviceName\":\"inventory\",\"command\":\"ReserveStock\"}",
      "nextStepId": "22222222-2222-4222-8222-222222222222",
      "compensationStepId": "44444444-4444-4444-8444-444444444444"
    },
    {
      "id": "22222222-2222-4222-8222-222222222222",
      "name": "Wait for approval",
      "stepType": "WAIT_EVENT",
      "metadata": "{\"eventName\":\"OrderApproved\"}",
      "nextStepId": "33333333-3333-4333-8333-333333333333"
    },
    {
      "id": "33333333-3333-4333-8333-333333333333",
      "name": "Charge payment",
      "stepType": "CALL",
      "metadata": "{\"serviceName\":\"payments\",\"command\":\"ChargePayment\"}",
      "compensationStepId": "55555555-5555-4555-8555-555555555555"
    },
    {
      "id": "44444444-4444-4444-8444-444444444444",
      "name": "Release inventory",
      "stepType": "CALL",
      "metadata": "{\"serviceName\":\"inventory\",\"command\":\"ReleaseStock\"}"
    },
    {
      "id": "55555555-5555-4555-8555-555555555555",
      "name": "Refund payment",
      "stepType": "CALL",
      "metadata": "{\"serviceName\":\"payments\",\"command\":\"RefundPayment\"}"
    }
  ]
}
```

Response: `{"definitionId":"<definition UUID>"}`. Reusing the same name/version or step IDs produces
`FailedPrecondition`.

### StartProcess

```json
{
  "definitionId": "<definition UUID>",
  "businessKey": "order-42",
  "inputData": "{\"orderId\":42,\"amount\":120.50}",
  "idempotencyKey": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
}
```

Response: `{"processInstanceId":"<process UUID>"}`. Input must be a JSON object; business keys are non-empty and at most
512 characters. An identical retry with the same idempotency key returns the original process ID and creates no
additional command. Reusing that key with different definition/business/input data produces `FailedPrecondition`. Input
JSON is compared as the supplied string, so whitespace/key-order changes count as different input.

### CompleteStep

Domain services call this after executing a command:

```json
{
  "processInstanceId": "<process UUID from command>",
  "stepDefinitionId": "<definition-step UUID from command>",
  "stepInstanceId": "<execution UUID from command>",
  "attempt": 0,
  "success": true,
  "outputData": "{\"reservationId\":\"r-42\"}",
  "correlationId": "inventory-result-r-42-attempt-0"
}
```

Response: `{}`. `stepInstanceId` is required; echo `attempt` from the command. Preserve the callback's correlation ID
when retrying the same callback. An already accepted callback returns `FailedPrecondition` without changing state. A
late response for an earlier attempt is rejected. `success: false` schedules a retry or triggers rollback after the
retry limit.

**Contract update:** existing clients must regenerate their protobuf types and supply `stepInstanceId` and `attempt`.
Checking only `stepDefinitionId` cannot distinguish execution attempts safely.

### SendEvent

```json
{
  "processInstanceId": "<process UUID>",
  "eventName": "OrderApproved",
  "payload": "{\"approvedBy\":\"operator-1\"}",
  "correlationId": "approval-42"
}
```

Response: `{}`. The process must currently be waiting on a matching `WaitEvent`. Names and correlation IDs must be
non-empty. The same normalized event name/correlation pair can be accepted only once per process. Rejected events are
not consumed. There is no early-event buffer; retry an event that arrived before its waiting step.

### CancelProcess

```json
{"processInstanceId":"<process UUID>"}
```

Response: `{}`. Cancels active forward work and schedules rollback of completed compensatable calls. Completed processes
cannot be cancelled. Repeating cancellation returns `FailedPrecondition`. Pending forward retries are cleared; unsent
forward commands are suppressed during outbox dispatch.

Cancellation is cooperative: a command already sent to a domain service may still execute. Only acknowledged successful
forward steps are automatically compensated. Handling uncertain in-flight outcomes requires domain-level reconciliation.

### GetProcessStatus

```json
{"processInstanceId":"<process UUID>"}
```

Returns definition/business identifiers, process status, current step-instance ID, UTC timestamps, and ordered
`stepHistory`. Each entry includes definition/execution IDs, status, output/error, timestamps, and retry count.
Compensation executions also appear in the history. A missing process returns `NotFound`.

For bounded history pages, supply `historyOffset`, `historyPageSize` (1..100) and `omitOutputData: true`.
The response includes `totalStepCount` and `historyOffset`. Omitted paging fields preserve full-history behavior.
Output-heavy pages may still need a smaller page size or a larger client receive limit.
Workers use `IsStepActive` with process/definition/execution IDs, attempt and compensation flag to check activity
without loading history. Deploy the scheduler's new RPCs before updating workers.

`GetProcessDefinition` accepts a definition `name` and positive `version` and returns its `definitionId`, or `NotFound`.
Use it to recover a committed registration whose response was lost; registration itself still rejects duplicate
versions.

Process statuses: `Created`, `Running`, `Waiting`, `Failed`, `Completed`, `Cancelled`.

Step statuses: `Pending`, `Running`, `Waiting`, `Completed`, `Failed`, `TimedOut`, `Cancelled`, `Compensated`.

### Error reference

| gRPC status          | Meaning                                                                                         |
|----------------------|-------------------------------------------------------------------------------------------------|
| `InvalidArgument`    | Invalid UUID, missing required value, malformed metadata/input, invalid graph                   |
| `NotFound`           | Process, definition, or step does not exist                                                     |
| `FailedPrecondition` | Duplicate callback/event, idempotency conflict, invalid current state/type, definition conflict |
| `PermissionDenied`   | Mapped authorization/domain permission error, if introduced by a caller                         |
| `Internal`           | Unexpected server/dependency failure                                                            |

## Client SDK

`WorkflowScheduler.Client` is a .NET 10 NuGet library that connects to the existing gRPC service. It provides a fluent
builder, typed JSON input/output, typed statuses, request deadlines, cancellation and dependency injection.

```csharp
using WorkflowScheduler.Client;

services.AddWorkflowSchedulerClient(options =>
    options.Address = new Uri("https://scheduler.example.com"));

// Inject IWorkflowSchedulerClient as scheduler.
var definition = Workflow.Define("approved-order", version: 1)
    .Call("inventory", "ReserveStock")
    .CompensateWith("inventory", "ReleaseStock")
    .WaitFor("Approved")
    .Call("payments", "Charge")
    .Build();

var registered = await scheduler.RegisterAsync(definition);
var processId = await scheduler.StartAsync(registered, new { orderId = 42 },
    businessKey: "order-42", idempotencyKey: requestId);
var status = await scheduler.GetStatusAsync(processId);
```

Registration rejects an existing name/version; save the returned ID and use it for later starts. Keep the same start
idempotency key or event/callback correlation ID when retrying an operation. The client does not automatically retry
mutating RPCs. The builder supports sequences, conditional branches with shared continuations and compensations.
Kafka command handling remains in the domain service. See
the [Client package documentation](src/WorkflowScheduler.Client/README.md)
for branches, callbacks, configuration and local NuGet packaging.

## Worker SDK

Add `WorkflowScheduler.Worker` to a domain service and register its command handlers. The package runs as a hosted
service
in that process and calls the scheduler through `WorkflowScheduler.Client`.

```csharp
builder.Services.AddWorkflowSchedulerClient(options =>
    options.Address = new Uri("https://scheduler.example.com"));
builder.Services.AddWorkflowSchedulerWorker(options =>
{
    options.ServiceName = "billing";
    options.BootstrapServers = "localhost:9092";
})
.Handle<ChargeRequest, ChargeResult, ChargeHandler>("Charge")
.Handle<ChargeRequest, ChargeResult, RefundHandler>("Refund");
```

Handlers implement `IWorkflowCommandHandler<TInput, TOutput>` and receive the original typed input, execution/attempt
IDs,
an idempotency key, compensation flag and cancellation token. Each message gets a DI scope. The worker checks the
current
execution before calling business code, reports success/failure through the Client and makes the Kafka offset eligible
for commit only after acknowledgement. It retries transient callback delivery using the same computed result and
correlation ID. Malformed envelopes go to a dead-letter topic. Business operations still need durable idempotency in
their own service across crashes. See [Worker package documentation](src/WorkflowScheduler.Worker/README.md) for handler
examples, delivery guarantees, timeouts, security and packaging.

## Domain-service integration

Commands are JSON-serialized `StepCommandEvent` records, published to `process.commands.<serviceName>`:

```json
{
  "ProcessInstanceId": "<process UUID>",
  "StepDefinitionId": "<definition-step UUID>",
  "StepInstanceId": "<execution UUID>",
  "Command": "ReserveStock",
  "InputData": "{\"orderId\":42}",
  "CorrelationId": "<execution UUID>",
  "Attempt": 0,
  "IsCompensation": false
}
```

Kafka JSON uses PascalCase contract properties. `InputData` contains the **original input string**, not an automatically
merged context. Output and event payloads are stored in history; they are not automatically forwarded to later calls or
used by conditions.

A domain service should:

1. Consume its command topic.
2. Deduplicate delivery for `(StepInstanceId, Attempt)` before repeating a side effect. If processing is already
   complete, resend the stored result rather than executing the operation again. Keep domain-level business idempotency
   across retries where required.
3. Execute `Command` using its own business rules and data.
4. Call `CompleteStep` with the matching process/definition/execution IDs and attempt.
5. Retry the callback with the same callback correlation ID if its acknowledgement is lost.

Kafka records are keyed by process-instance ID. The command `CorrelationId` remains stable for the step; `Attempt`
distinguishes an intentional retry from transport redelivery. Compensation commands set `IsCompensation: true` and have
distinct execution IDs.

`WorkflowScheduler.Contracts.Grpc` contains both the gRPC protocol types and the Kafka command envelope shared by the
scheduler and Worker SDK. Callbacks and external events are submitted through gRPC. Process-lifecycle events are not
published.

## Reliability and consistency

### Durable idempotency

Start receipts live in `start_requests`, not an expiring Redis flag. The receipt contains the idempotency key, process
ID, and SHA-256 request fingerprint and is committed with the process/outbox records. A failed start does not reserve
the key. Receipts currently have no automatic expiry or cleanup.

### Concurrency

The transaction behavior acquires a PostgreSQL transaction-scoped advisory lock for a process command, a start key, or a
definition name/version. The lock remains held through `SaveChanges` and commit/rollback. Concurrent API instances use
the same durable lock namespace. Unrelated processes can progress independently. Database uniqueness constraints
additionally protect receipts, definition versions, step IDs, and event identities.

Redis is only an optimization. Cache connection/timeout failures and malformed cache snapshots fall back to PostgreSQL.
Cache writes are best-effort. A Redis outage cannot erase start receipts or release a process mutation lock.

### Transactional outbox

The scheduler writes a command to `outbox_messages` in the same transaction as its process changes. A dispatcher claims
up to 100 due messages using `FOR UPDATE SKIP LOCKED`, commits a one-minute lease, then publishes outside the database
transaction. Process statuses are loaded once per batch. Each publish has a ten-second timeout; unfinished claims are
released, and a crashed dispatcher's claims become available after lease expiry. Ownership checks fence delivery
updates.
Full batches drain immediately; otherwise the worker polls every two seconds.

Failures retain a bounded error and retry after 2, 4, 8, 16, 32, then 60 seconds. Malformed/oversized messages and
messages
that fail eight attempts are quarantined with `FailedAt`; they do not block newer commands. Inspect the error and fix
its
cause before resetting `FailedAt`, `NextAttemptAt` and `Attempts` for the affected message. Quarantine does not fail or
complete its workflow automatically. A successful delivery clears the error.

Before deploying this version to an existing database, stop old scheduler instances and run
`dotnet run --project src/WorkflowScheduler -- --initialize-database`. This preserves existing rows and applies the
idempotent [outbox schema upgrade](database/001_outbox_delivery.sql). The SQL can also be applied by deployment tooling.

Delivery is **at least once**. A crash after Kafka acknowledges a record but before PostgreSQL records `ProcessedAt` may
cause redelivery. Kafka producer idempotence does not make the database/broker boundary exactly once. Domain-service
deduplication remains necessary.

### Retry scheduling

A failed call schedules the next attempt for 30 seconds later. The process remains `Waiting` and its step becomes
`Pending`. A dedicated worker polls due retries every two seconds, rechecks state under the process transaction lock,
and emits the next attempt. A retry is not dispatched directly from the failure callback.

The current default is one initial attempt plus at most three retries. The same step-instance ID is retained; `Attempt`
equals the step's retry count. Early or stale callbacks cannot complete a pending step or a newer attempt. Retry
deadlines survive scheduler restarts. Compensation calls use the same retry mechanism.

## Testing

The tests use xUnit, explicit Arrange/Act/Assert sections, and fresh scopes to verify persisted state.
Integration and EndToEnd tests automatically start PostgreSQL, Redis and Kafka through Testcontainers.
Docker must be running and accessible to the test process; no Compose setup or opt-in environment variable is needed.
Container startup failures fail the run with a diagnostic error.

```bash
# Complete suite; containers start automatically and are reused for the whole run.
dotnet test WorkflowScheduler.slnx

# Fast unit tests; Docker is not required and no containers are started.
dotnet test WorkflowScheduler.slnx --filter 'Category=Unit'

# All container-backed layers in one invocation share the same infrastructure.
dotnet test WorkflowScheduler.slnx --filter 'Category=Integration|Category=EndToEnd'

# Run one layer for diagnosis.
dotnet test WorkflowScheduler.slnx --filter 'Category=Integration'
dotnet test WorkflowScheduler.slnx --filter 'Category=EndToEnd'

# Full suite with coverage (Docker required).
dotnet test WorkflowScheduler.slnx \
  --collect:'XPlat Code Coverage' --settings coverage.runsettings
```

| Layer       | Exercises                                                                                                                                                                                                                      | Dependencies                                                                                                                   |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Unit        | Domain transitions, retry limits, condition operators/culture, validation, cache serialization and failure handling                                                                                                            | Isolated test doubles                                                                                                          |
| Integration | PostgreSQL transactions/advisory locks, concurrent commands, rollback, retries, saga ordering, Redis cache, outbox recovery/`SKIP LOCKED`, isolation, Worker offsets, duplicate delivery, dead letters and shutdown/redelivery | Real PostgreSQL, Redis and Kafka; decorators inject deterministic publication/callback failures                                |
| EndToEnd    | gRPC over HTTP/2, Client/Worker typed handlers, concurrent starts, workflow transitions/events, API errors, cancellation, compensation, scheduled retry and lost callback acknowledgements                                     | Kestrel on an ephemeral port, real PostgreSQL/Redis/Kafka, production adapters, outbox/retry workers and the hosted Worker SDK |

One xUnit collection fixture starts the three containers concurrently once per test process. Schemas and Kafka topics
are created once. Pinned container images match `compose.yaml`, so development and tests share the local image cache.
The integration service provider and database connection pools are reused. Container tests run
sequentially within that collection so resets cannot race other test cases; unit test collections retain parallelism.
Integration tests use a dedicated PostgreSQL database and Redis database 0. E2E tests use another PostgreSQL database
and Redis database 1. PostgreSQL tables are cleared with a single `TRUNCATE`, and only the corresponding Redis database
is flushed before each test. Containers are disposed at the end of the collection, including partial startup failures.

Each E2E test creates a lightweight Kestrel host and disposes it asynchronously, stopping its production workers before
the next database reset. Kafka consumers capture each topic's end offset before triggering the test workflow and use
explicit partition assignment. This avoids historical records and consumer-group rebalances while keeping the broker
and topics alive. gRPC calls, broker reads and state polling have bounded timeouts. Retry tests advance the persisted
retry deadline
so the real worker can run without waiting 30 seconds.

The scheduler runs in the test process with real network connections. Client/Worker scenarios execute the actual hosted
Worker SDK and typed handlers. Other scenarios simulate domain services by consuming Kafka commands and sending gRPC
callbacks. Deployment images, TLS and separately deployed domain services require additional
deployment tests. GitHub Actions runs the complete suite in one invocation with Docker and uploads TRX and Cobertura
coverage artifacts.

## Limitations

- No parallel branches, timers, cron schedules, or durable early-event inbox. Cycles of automatic conditions are
  rejected; loops through waiting steps are supported.
- No public per-step timeout configuration or automatic timeout worker yet. Domain timeout fields are not a delivery
  guarantee; a missing callback can leave a call waiting indefinitely.
- No automatic output/context mapping between steps; conditions read the original input.
- No automatic recovery API for exhausted compensation. Inspect `saga_compensation_logs` and domain-service state before
  intervention.
- Cancellation does not stop commands already executing in another service.
- No published process-lifecycle events, broker callback consumer, metrics dashboard, retention policy, or production
  migration history.
- The initializer creates empty databases and applies the included outbox upgrade. Other schema changes still require
  explicit migration scripts; there is no general migration history.
- Authentication and authorization are not implemented in the gRPC service.

## License

[MIT](LICENSE).
