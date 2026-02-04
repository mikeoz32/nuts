# Event Sourcing Implementation Plan

This document defines the implementation epics, tasks, and subtasks for delivering the
event-sourcing feature in Nuts. Each subtask follows the **what / why / DoD** format.
Projections are intentionally out of scope for this phase.

---

## Epic 1: Event Sourcing Contracts & Core API

### Task 1.1: Define ES message envelopes and subjects
**Goal:** Lock down the NATS/JetStream message formats and subject layout.

**Subtasks**
- **what:** Specify subject patterns for commands and events.
  - **why:** Consistent subjects allow routing, filtering, and stream configuration.
  - **DoD:** `docs/event_sourcing.md` references a single canonical subject pattern for:
    - commands: `{app}.es.command.{aggregate_type}`
    - events: `{app}.es.event.{aggregate_type}.{aggregate_id}`

- **what:** Define standard message envelope fields for commands and events.
  - **why:** Ensures interoperability between client and server and enables tracing.
  - **DoD:** The document includes a required field list (e.g. `id`, `timestamp`,
    `aggregate_id`, `aggregate_type`, `name`, `payload`, `correlation_id`, `causation_id`)
    and clearly marks required vs optional.

- **what:** Document error response envelope for rejected commands.
  - **why:** Callers need structured error feedback to handle conflicts/validation failures.
  - **DoD:** Error envelope includes `error.code`, `error.message`, `error.details`
    and correlates with the originating command `id`.

### Task 1.2: Define aggregate and command handler API
**Goal:** Provide a minimal developer-facing API for aggregates in Nuts.

**Subtasks**
- **what:** Define `Aggregate` base protocol (apply + state access).
  - **why:** Ensures a consistent interface for loading/replaying and command handling.
  - **DoD:** API definition covers:
    - `apply(event)` for state mutation
    - `state` serialization contract
    - `version`/`last_sequence` tracking

- **what:** Define command handler signature for aggregate commands.
  - **why:** Server needs a uniform callable to produce domain events from commands.
  - **DoD:** Handler signature is documented as:
    - `async def handle(command, aggregate) -> list[Event]`
    - includes validation + produces domain events

- **what:** Define event type declaration format.
  - **why:** Enables serialization, schema evolution, and event metadata consistency.
  - **DoD:** Event structure includes `name`, `payload`, and metadata fields and a
    JSON serialization example.

---

## Epic 2: JetStream Event Store

### Task 2.1: Implement JetStream stream provisioning
**Goal:** Ensure streams exist for event storage per aggregate type or per app.

**Subtasks**
- **what:** Add a provisioning step in ES feature startup.
  - **why:** Streams must exist before events can be appended.
  - **DoD:** On startup, ES feature creates or validates a stream (e.g. `NUTS_ES`)
    with subjects matching `{app}.es.event.*`.

- **what:** Add configuration knobs for stream retention and max age.
  - **why:** Event retention policy must be configurable per deployment.
  - **DoD:** ES feature accepts configuration values (retention, max_age, max_bytes)
    and documents defaults.

### Task 2.2: Append events with sequence guarantees
**Goal:** Store events with per-aggregate sequence and optimistic concurrency.

**Subtasks**
- **what:** Implement `EventStore.append(aggregate_id, expected_version, events)`.
  - **why:** Prevents race conditions and enforces aggregate consistency.
  - **DoD:** Append fails if the last sequence does not match `expected_version`,
    producing a well-defined concurrency error.

- **what:** Assign and persist sequence numbers for events.
  - **why:** Ordering is required for replay and snapshotting.
  - **DoD:** Each event is persisted with an incrementing `sequence` value for
    its aggregate, and returned to the caller.

### Task 2.3: Load event streams for an aggregate
**Goal:** Support replaying from the last snapshot sequence.

**Subtasks**
- **what:** Implement `EventStore.load(aggregate_id, from_sequence)`.
  - **why:** Rebuild aggregate state by replaying events.
  - **DoD:** Loading returns events ordered by sequence >= `from_sequence + 1`.

- **what:** Ensure backpressure and batch size controls.
  - **why:** Protects memory when aggregates have large histories.
  - **DoD:** Load supports `batch_size` with streaming iteration or pagination.

---

## Epic 3: Snapshot Storage (JetStream KV)

### Task 3.1: Snapshot bucket provisioning
**Goal:** Create KV buckets for snapshots per aggregate type.

**Subtasks**
- **what:** Provision a snapshot KV bucket on startup.
  - **why:** Snapshots speed up aggregate reconstruction.
  - **DoD:** Bucket is created/validated for each configured aggregate type.

- **what:** Define snapshot data schema.
  - **why:** Needed to serialize state and the last applied sequence.
  - **DoD:** Snapshot value includes `state`, `last_sequence`, `timestamp`, and
    `aggregate_type`.

### Task 3.2: Snapshot load and save
**Goal:** Load from snapshots and persist new ones based on policy.

**Subtasks**
- **what:** Implement `SnapshotStore.load(aggregate_id)`.
  - **why:** Avoid replaying entire history when a snapshot exists.
  - **DoD:** Returns `(state, last_sequence)` or `None`.

- **what:** Implement `SnapshotStore.save(aggregate_id, snapshot)`.
  - **why:** Allow periodic snapshots for large aggregates.
  - **DoD:** Stores snapshot atomically and returns the stored revision.

- **what:** Implement snapshot policy (every N events).
  - **why:** Controls snapshot frequency for performance vs storage.
  - **DoD:** Policy is configurable and applied after successful event append.

---

## Epic 4: ES Feature Runtime (Server Integration)

### Task 4.1: Command subscription and dispatch
**Goal:** Receive commands over NATS and route to aggregate handlers.

**Subtasks**
- **what:** Subscribe to `{app}.es.command.{aggregate_type}`.
  - **why:** Provides a consistent entrypoint for command requests.
  - **DoD:** ES feature creates subscriptions per aggregate type on startup.

- **what:** Parse command messages and validate envelopes.
  - **why:** Ensures data integrity and proper error responses.
  - **DoD:** Invalid messages yield error responses with `error.code=invalid_command`.

### Task 4.2: Aggregate reconstruction and command handling
**Goal:** Load aggregate state, handle commands, and produce events.

**Subtasks**
- **what:** Rebuild aggregate from snapshot + event replay.
  - **why:** Command handling must operate on current state.
  - **DoD:** The aggregate is reconstructed using snapshot (if present)
    and replayed events from `last_sequence + 1`.

- **what:** Call aggregate command handler to generate events.
  - **why:** Aggregate logic defines allowed transitions.
  - **DoD:** Handler returns a list of domain events or raises a domain error.

- **what:** Translate domain errors to command error responses.
  - **why:** Callers need structured error feedback for rejected commands.
  - **DoD:** Errors are mapped to `error.code` values (e.g. `validation_error`,
    `conflict_error`, `not_found`) with details.

### Task 4.3: Event append + response
**Goal:** Persist events and respond to the command requester.

**Subtasks**
- **what:** Append events using optimistic concurrency.
  - **why:** Prevents conflicting writes to the same aggregate.
  - **DoD:** Append includes `expected_version` and returns new sequence numbers.

- **what:** Publish response with event metadata.
  - **why:** Client needs new version/sequence for subsequent commands.
  - **DoD:** Response includes `aggregate_id`, `new_version`, and event list.

- **what:** Trigger snapshot policy after append.
  - **why:** Keep aggregate reconstruction efficient.
  - **DoD:** Snapshot save happens after a successful append when policy threshold is met.

---

## Epic 5: Configuration & Observability

### Task 5.1: ES feature configuration schema
**Goal:** Provide a structured configuration for ES feature behavior.

**Subtasks**
- **what:** Define configuration options for stream + snapshot policies.
  - **why:** Allows deployment-specific tuning.
  - **DoD:** Config includes stream name, subjects, retention, snapshot interval,
    and batch size with defaults documented.

- **what:** Document environment variable mapping (if applicable).
  - **why:** Operators may configure via env vars in containers.
  - **DoD:** Environment variable mapping is described in docs.

### Task 5.2: Logging and metrics hooks
**Goal:** Provide visibility into ES operations.

**Subtasks**
- **what:** Add structured logs for command processing stages.
  - **why:** Debugging and auditing require visibility into the flow.
  - **DoD:** Logs include command id, aggregate id/type, outcome, and timing.

- **what:** Add basic counters/timers for throughput and latency.
  - **why:** Supports operational monitoring and alerting.
  - **DoD:** Metrics record counts for command processed, errors, and append latency.

---

## Epic 6: Test Coverage & Example Integration

### Task 6.1: Unit tests for store components
**Goal:** Validate EventStore and SnapshotStore behavior.

**Subtasks**
- **what:** Test append + concurrency conflict handling.
  - **why:** Ensures correct behavior under concurrent updates.
  - **DoD:** A test asserts conflict error on version mismatch.

- **what:** Test snapshot load/save and replay correctness.
  - **why:** Validates state reconstruction logic.
  - **DoD:** A test shows aggregate state reconstructed from snapshot + events.

### Task 6.2: Integration test for ES feature
**Goal:** Confirm end-to-end command handling and event storage.

**Subtasks**
- **what:** Create an example aggregate in `example/`.
  - **why:** Demonstrates intended API usage.
  - **DoD:** Example includes an aggregate, a command, and an event handler.

- **what:** Run an integration test using NATS + JetStream.
  - **why:** Ensures feature works with real infrastructure.
  - **DoD:** Test sends a command, receives response, and verifies event stored.

---

## Epic 7: Documentation & Developer Guidance

### Task 7.1: Update architecture docs
**Goal:** Keep docs aligned with the implementation plan.

**Subtasks**
- **what:** Cross-link `docs/event_sourcing.md` to this implementation plan.
  - **why:** Helps contributors understand execution steps.
  - **DoD:** `docs/event_sourcing.md` links to `docs/implementation.md`.

- **what:** Add a “Getting started” snippet for creating aggregates.
  - **why:** Reduces onboarding friction for new contributors.
  - **DoD:** Documentation includes a minimal aggregate example and command flow.
