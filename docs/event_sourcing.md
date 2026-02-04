# Event sourcing architecture & API proposal

This document describes the proposed event-sourcing architecture for Nuts and the
public API that applications will implement.

For the execution plan and task breakdown, see [docs/implementation.md](implementation.md).

## Goals
- Provide a consistent ASGI-inspired execution model for commands and events.
- Use NATS JetStream as the event store.
- Use JetStream KV for snapshots (projections are out of scope for now).
- Keep feature modules small and async-first.

## High-level architecture

### Components
- **ES Feature** (`nuts/server/es/feature.py`): server extension that subscribes to
  command subjects, loads aggregate state, executes commands, and appends events.
- **Event Store** (JetStream): stores immutable domain events per aggregate.
- **Snapshot Store** (JetStream KV): stores serialized aggregate snapshots with the
  last applied sequence.

### Subject layout
- Command requests: `{app}.es.command.{aggregate_type}`
- Events stream: `{app}.es.event.{aggregate_type}.{aggregate_id}`

This layout allows fast filtering of events by aggregate type and ID.

### Message envelopes
All ES interactions are typed by `scope['type'] = 'es'`, while messages are
standardized:

**Command request** (`es.command.request`)
```json
{
  "type": "es.command.request",
  "aggregate_type": "orders",
  "aggregate_id": "order-123",
  "command": "CreateOrder",
  "payload": {"customer_id": "c-1", "total": 42},
  "metadata": {"correlation_id": "...", "causation_id": "..."}
}
```

**Command response** (`es.command.response`)
```json
{
  "type": "es.command.response",
  "events": [
    {"event": "OrderCreated", "payload": {"customer_id": "c-1", "total": 42}}
  ]
}
```

**Event apply request** (`es.event.request`)
```json
{
  "type": "es.event.request",
  "event": "OrderCreated",
  "payload": {"customer_id": "c-1", "total": 42}
}
```

**Event apply response** (`es.event.response`)
```json
{
  "type": "es.event.response",
  "aggregate": {"id": "order-123", "status": "created", "total": 42}
}
```

## Aggregate API in Nuts

Aggregates are declared by providing command and event handlers. The framework
manages loading snapshots, replaying events, and saving new events. The aggregate
API is intentionally explicit to keep business logic simple.

### Aggregate definition
```python
from nuts import Aggregate, Command, Event

class OrderAggregate(Aggregate):
    aggregate_type = "orders"

    @Command("CreateOrder")
    def create_order(self, payload):
        return [Event("OrderCreated", payload)]

    @Event("OrderCreated")
    def apply_order_created(self, payload):
        self.state["status"] = "created"
        self.state["total"] = payload["total"]
```

### Expected behavior
- **Command handler** returns one or more events (or raises a domain error).
- **Event handler** mutates aggregate state deterministically.
- State is persisted by snapshotting based on a policy (e.g., every N events).

## ES feature flow
1. Receive `es.command.request`.
2. Resolve aggregate type and id.
3. Load snapshot from KV (if present).
4. Replay events after snapshot sequence from JetStream.
5. Execute command handler to produce events.
6. Append events to JetStream with metadata.
7. Optionally create snapshot and persist to KV.

## Suggested snapshot policy
- Snapshot after N events (e.g., 50) per aggregate.
- Store `last_sequence` with snapshot to resume replay.

## Open questions
- Exact serialization format (JSON vs msgpack).
- Error taxonomy for command failures.
- Concurrency strategy (expected version vs optimistic append).
