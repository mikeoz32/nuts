# AGENTS.md

## Project overview
- **nuts** is a lightweight NATS-based microservices framework with an ASGI-inspired RPC interface.
- Core package lives in `nuts/`.
- Server extensions live under `nuts/server/` and are composed as `ServerFeature` implementations.

## Key paths
- `nuts/nuts.py`: application entrypoint and RPC routing.
- `nuts/server/nsgi.py`: server runtime with feature lifecycle hooks.
- `nuts/server/rpc/`: RPC feature implementation.
- `nuts/server/es/`: event-sourcing feature (work in progress).

## Development
- Run example service: `hatch run server`
- Run tests: `hatch run test`

## Style notes
- Prefer small, focused feature modules under `nuts/server/<feature>/`.
- Follow existing async patterns (async startup/shutdown, message handlers).
- Avoid blocking calls in request handlers.
