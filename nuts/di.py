from typing import Annotated, Any, Callable, Dict, get_args, get_origin
import inspect


class Bean:
    def __init__(self, call: Callable) -> None:
        self.call = call
        self.signature = inspect.signature(call)
        self.context = dict()

        self.beans = dict()

        for value in self.signature.parameters.values():
            annotation = value.annotation
            origin = get_origin(annotation)
            if origin == Annotated:
                ann_type, maybe_bean = get_args(annotation)
                if isinstance(maybe_bean, Bean):
                    self.beans[value.name] = maybe_bean

    def add_context(self, context: Dict) -> "Bean":
        self.context.update(context)
        return self

    async def __call__(self, *args: Any, **kwds: Any) -> Any:
        for name, bean in self.beans.items():
            if name in self.context.keys():
                continue
            bean.add_context(self.context)
            kwds.update({name: await bean()})
        for value in self.signature.parameters.keys():
            if value in self.context.keys():
                kwds[value] = self.context.get(value)
        return await self.call(*args, **kwds)

    def __call__(self, *args: Any, **kwds: Any) -> Any:  # noqa
        for name, bean in self.beans.items():
            if name in self.context.keys():
                continue
            bean.add_context(self.context)
            kwds.update({name: bean()})
        for value in self.signature.parameters.keys():
            if value in self.context.keys():
                kwds[value] = self.context.get(value)
        return self.call(*args, **kwds)
