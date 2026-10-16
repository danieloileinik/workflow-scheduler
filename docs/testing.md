# Testing

[Back to the project overview](../README.md#testing). Run commands from the repository root.

The tests use xUnit and fresh scopes to verify persisted state.
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

## Coverage reporting

The combined report measures the Scheduler, Client SDK and Worker SDK; generated files under `obj` are excluded by
[`coverage.runsettings`](../coverage.runsettings). The order demo has a separate test suite and is not part of these
figures. Line coverage measures executed lines; branch coverage measures executed alternatives of conditional logic.
Neither metric guarantees that every business scenario or failure mode is tested.

The [Tests workflow](../.github/workflows/tests.yml) runs the suite in Release mode and uploads TRX and Cobertura files
in the `test-results` artifact. After a successful run, [`update-test-summary.py`](../scripts/update-test-summary.py)
reads those files, updates the marked summary in the main README and writes the same summary to the Actions run page.
Only a successful non-forced push run on the current `main` head can commit the generated README, using
`docs: update test coverage`. PR runs produce a summary without pushing changes. A newer commit on `main` causes the
older run to skip publication. Test execution retains read-only repository permissions; only the publication job can
write repository contents.

To refresh the summary locally after collecting coverage:

```bash
python3 scripts/update-test-summary.py
```

In CI, the summary also links to the exact run and tested commit. Locally, it is labelled as a local run. The script
requires one distinct combined Cobertura report and one TRX result file; remove older `TestResults` before collecting
fresh results. Byte-identical copies of the coverage report are counted once.
