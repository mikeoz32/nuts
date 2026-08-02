from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence


_MISSING = object()


class Command:
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, "__nuts_command__", self.name)
        return func


@dataclass(frozen=True)
class Event:
    name: str
    payload: Any = _MISSING

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, "__nuts_event__", self.name)
        return func

    def to_dict(self) -> Dict[str, Any]:
        if self.payload is _MISSING:
            raise ValueError("Event payload is required when emitting events.")
        return {"name": self.name, "payload": self.payload}


class Aggregate:
    aggregate_type: str = ""

    def __init__(self, state: Optional[Mapping[str, Any]] = None, aggregate_id: str | None = None) -> None:
        self.state: Dict[str, Any] = dict(state or {})
        self.id = aggregate_id

    @classmethod
    def command_handlers(cls) -> Dict[str, Callable[..., Any]]:
        handlers: Dict[str, Callable[..., Any]] = {}
        for _, method in inspect.getmembers(cls, predicate=callable):
            command_name = getattr(method, "__nuts_command__", None)
            if command_name:
                handlers[command_name] = method
        return handlers

    @classmethod
    def event_handlers(cls) -> Dict[str, Callable[..., Any]]:
        handlers: Dict[str, Callable[..., Any]] = {}
        for _, method in inspect.getmembers(cls, predicate=callable):
            event_name = getattr(method, "__nuts_event__", None)
            if event_name:
                handlers[event_name] = method
        return handlers


async def _maybe_await(result: Any) -> Any:
    if asyncio.iscoroutine(result):
        return await result
    return result


def _normalize_events(events: Any) -> List[Dict[str, Any]]:
    if events is None:
        return []
    if isinstance(events, Event):
        return [events.to_dict()]
    if isinstance(events, Mapping):
        return [dict(events)]
    if not isinstance(events, Sequence):
        raise TypeError("Command handlers must return an event or list of events.")
    normalized: List[Dict[str, Any]] = []
    for event in events:
        if isinstance(event, Event):
            normalized.append(event.to_dict())
        elif isinstance(event, Mapping):
            normalized.append(dict(event))
        else:
            raise TypeError("Event must be a dict or Event instance.")
    return normalized


def _call_with_metadata(method: Callable[..., Any], instance: Aggregate, payload: Any, metadata: Dict[str, Any]):
    signature = inspect.signature(method)
    if "metadata" in signature.parameters:
        return method(instance, payload, metadata=metadata)
    return method(instance, payload)


def register_aggregate(app: Any, aggregate_cls: type[Aggregate]) -> None:
    aggregate_type = aggregate_cls.aggregate_type
    if not aggregate_type:
        raise ValueError("Aggregate.aggregate_type must be set.")

    for command_name, method in aggregate_cls.command_handlers().items():
        async def command_handler(payload, aggregate, metadata=None, _method=method):
            metadata = metadata or {}
            aggregate_id = metadata.get("aggregate_id")
            instance = aggregate_cls(aggregate or {}, aggregate_id=aggregate_id)
            result = await _maybe_await(
                _call_with_metadata(_method, instance, payload, metadata)
            )
            return _normalize_events(result)

        app.add_command(aggregate_type, command_name, command_handler)

    for event_name, method in aggregate_cls.event_handlers().items():
        async def event_handler(payload, aggregate, _method=method):
            instance = aggregate_cls(aggregate or {})
            result = await _maybe_await(_method(instance, payload))
            return instance.state if result is None else result

        app.add_aggregate_event(aggregate_type, event_name, event_handler)
