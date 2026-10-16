# Process definitions and gRPC API

[Back to the project overview](../README.md).

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
`src/WorkflowScheduler.Contracts.Grpc/processes.proto`](../src/WorkflowScheduler.Contracts.Grpc/processes.proto). Service
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
