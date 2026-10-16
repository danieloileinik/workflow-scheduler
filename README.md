# Workflow Scheduler

[![Tests](https://github.com/danieloileinik/workflow-scheduler/actions/workflows/tests.yml/badge.svg)](https://github.com/danieloileinik/workflow-scheduler/actions/workflows/tests.yml)
[Coverage and test scenarios](#testing)

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
- [Process definitions and gRPC API](#process-definitions-and-grpc-api)
- [Client SDK](#client-sdk)
- [Worker SDK](#worker-sdk)
- [Reliability and consistency](#reliability-and-consistency)
- [Testing](#testing)
- [Limitations](#limitations)

## Architecture

| Component | Responsibility |
|-----------|----------------|
| Scheduler | Workflow definitions, execution state, gRPC endpoints, retries and saga compensation |
| PostgreSQL | Durable process history, idempotency receipts, transaction locks and command outbox |
| Redis | Optional cache for immutable workflow definitions |
| Kafka | At-least-once command delivery to domain services |
| Client SDK | Fluent definitions, typed payloads/statuses and a reusable gRPC client |
| Worker SDK | Typed command handlers, scoped execution, callbacks and offset acknowledgement |

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

## Process definitions and gRPC API

Definitions contain versioned `Call`, `WaitEvent` and `Condition` steps. Calls publish commands to domain services;
events resume waiting processes; conditions select a branch using the original input. Completed calls can declare
compensations, executed in reverse order after cancellation or exhausted retries.

Use the Client SDK below, or see the [gRPC API reference](docs/grpc-api.md) for metadata, raw requests, error codes and
the domain-service command/callback contract.

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
service in that process and calls the scheduler through `WorkflowScheduler.Client`.

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
IDs, an idempotency key, compensation flag and cancellation token. Each message gets a DI scope. The worker checks the
current execution before calling business code, reports success/failure through the Client and makes the Kafka offset eligible
for commit only after acknowledgement. It retries transient callback delivery using the same computed result and
correlation ID. Malformed envelopes go to a dead-letter topic. Business operations still need durable idempotency in
their own service across crashes. See [Worker package documentation](src/WorkflowScheduler.Worker/README.md) for handler
examples, delivery guarantees, timeouts, security and packaging.

## Reliability and consistency

- **Durable state:** process changes, idempotency receipts and outbox commands commit in one PostgreSQL transaction.
  Transaction-scoped advisory locks serialize competing changes to the same process or request identity.
- **At-least-once delivery:** the outbox retries Kafka publication and recovers expired claims. Domain services must
  persist their own deduplication receipts because a crash can cause command redelivery.
- **Retries and compensation:** failed calls allow three delayed retries, with deadlines persisted across restarts.
  Cancellation and exhausted retries compensate acknowledged successful calls in reverse order.
- **Optional cache:** Redis failures fall back to PostgreSQL; Redis does not own idempotency or concurrency locks.

See [reliability details](docs/reliability.md) for dispatch leases, backoff, quarantine and database upgrade instructions.

## Testing

<!-- test-summary:start -->
**239 tests passed** · **95.50% line coverage** (2186/2289) · **83.47% branch coverage** (727/871).
Unit: **163** · Integration: **57** · E2E: **19**.

Combined coverage of Scheduler, Client SDK and Worker SDK; generated `obj` files are excluded.
Measured by [CI](https://github.com/danieloileinik/workflow-scheduler/actions/runs/38822240589).
<!-- test-summary:end -->

| Layer | What is tested |
|-------|----------------|
| Unit | Process/step transitions, validation, conditions, cache failure handling and Client/Worker SDK logic |
| Integration | Transactions, concurrent requests, idempotency, Redis, outbox recovery, retries, Kafka offsets and dead letters |
| EndToEnd | gRPC → Kafka → Worker → callback, typed workflows, events, cancellation, compensation and lost acknowledgements |

Integration and E2E tests use real PostgreSQL, Redis and Kafka through Testcontainers, shared across the run.
Docker must be running; no Compose setup is required. Unit tests run without Docker.

```bash
dotnet test WorkflowScheduler.slnx
dotnet test WorkflowScheduler.slnx --filter 'Category=Unit'
```

See [testing details](docs/testing.md) for coverage collection, layer filters, container isolation and CI reports.
The [order demo](samples/order-demo/README.md) also includes a separate test suite and deployed HTTP smoke scenarios.

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

