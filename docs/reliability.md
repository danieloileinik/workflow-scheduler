# Reliability and consistency

[Back to the project overview](../README.md). Run commands from the repository root.

## Durable idempotency

Start receipts live in `start_requests`, not an expiring Redis flag. The receipt contains the idempotency key, process
ID, and SHA-256 request fingerprint and is committed with the process/outbox records. A failed start does not reserve
the key. Receipts currently have no automatic expiry or cleanup.

## Concurrency

The transaction behavior acquires a PostgreSQL transaction-scoped advisory lock for a process command, a start key, or a
definition name/version. The lock remains held through `SaveChanges` and commit/rollback. Concurrent API instances use
the same durable lock namespace. Unrelated processes can progress independently. Database uniqueness constraints
additionally protect receipts, definition versions, step IDs, and event identities.

Redis is only an optimization. Cache connection/timeout failures and malformed cache snapshots fall back to PostgreSQL.
Cache writes are best-effort. A Redis outage cannot erase start receipts or release a process mutation lock.

## Transactional outbox

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
idempotent [outbox schema upgrade](../database/001_outbox_delivery.sql). The SQL can also be applied by deployment tooling.

Delivery is **at least once**. A crash after Kafka acknowledges a record but before PostgreSQL records `ProcessedAt` may
cause redelivery. Kafka producer idempotence does not make the database/broker boundary exactly once. Domain-service
deduplication remains necessary.

## Retry scheduling

A failed call schedules the next attempt for 30 seconds later. The process remains `Waiting` and its step becomes
`Pending`. A dedicated worker polls due retries every two seconds, rechecks state under the process transaction lock,
and emits the next attempt. A retry is not dispatched directly from the failure callback.

The current default is one initial attempt plus at most three retries. The same step-instance ID is retained; `Attempt`
equals the step's retry count. Early or stale callbacks cannot complete a pending step or a newer attempt. Retry
deadlines survive scheduler restarts. Compensation calls use the same retry mechanism.
