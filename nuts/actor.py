"""
* TODO: use https://github.com/RuneBlaze/atomicx for atomic values
"""

from asyncio import Queue, QueueEmpty
import asyncio
from dataclasses import dataclass
from logging import DEBUG, basicConfig, getLogger
from uuid import uuid4
from typing import Any, Callable, Coroutine, List, Protocol

LOCAL_ADDRESS = "nohost"

logger = getLogger("nuts.actor")
basicConfig(level=DEBUG)


class ActorRef: ...


SpawnerFunc = Callable[["ActorSystem", str, "Props", "SpawnerContext"], None]


def default_spawner(
    actor_system: "ActorSystem",
    id: str,
    props: "Props",
    parent_context: "SpawnerContext",
):
    context = ActorContext(
        actor_system, props, parent_context and parent_context.get_self()
    )
    mailbox = props.create_mailbox()

    process = ActorProcess(mailbox)
    pid = actor_system._process_registry.add(process, id)

    context._self = pid

    mailbox.register_handlers(context, props._dispatcher)

    logger.debug(f"Actor[{context._behavior.__class__.__name__}] spawned with {pid}")

    return pid


class PID:
    def __init__(self, id: str, address: str = LOCAL_ADDRESS) -> None:
        self._address = address
        self._id = id

    @property
    def id(self):
        return self._id

    def __repr__(self) -> str:
        return f"[PID({self.id})] at {self._address}"

    def send_user_message(self, actor_system: "ActorSystem", message: Any):
        proc: Process = actor_system._process_registry.get(self)
        if proc:
            proc.send_user_message(self, message)
        else:
            logger.debug(f"Process not found for pid[{self}]")


class Process(Protocol):
    def send_user_message(self, pid: PID, message: Any): ...
    def send_system_message(self, pid: PID, message: Any): ...
    def stop(self, pid: PID): ...


class AsyncDispatcher:
    def schedule(self, fn: Coroutine):
        asyncio.create_task(fn)

    def throughput(self):
        return 10

    def __repr__(self) -> str:
        return "AsyncDispatcher"


class Mailbox(Protocol):
    def register_handlers(self, invoker, dispatcher): ...

    def post_user_message(self, message: Any): ...


class DefaultMailbox(Mailbox):
    def __init__(self) -> None:
        self._user_mailbox = Queue()
        self._system_mailbox = Queue()
        self._dispatcher = AsyncDispatcher()
        self._invoker = None
        self._scheduler_status = 0
        self._user_messages = 0

    def register_handlers(self, invoker, dispatcher):
        logger.debug(f"Registering invoker [{invoker}] and dispatcher [{dispatcher}]")
        self._invoker = invoker
        self._dispatcher = dispatcher

    def post_user_message(self, message: Any):
        self._user_mailbox.put_nowait(message)
        self._user_messages = self._user_messages + 1
        self.schedule()

    def schedule(self):
        if self._scheduler_status == 0:
            self._scheduler_status = 1
            self._dispatcher.schedule(self.process_messages())

    async def process_messages(self):
        logger.debug(f"processing messages {self._user_mailbox.qsize()}")

        try:
            while message := self._user_mailbox.get_nowait():
                logger.debug(f"Processing message {message}")
                await self._invoker.invoke_user_message(message)
        except QueueEmpty:
            logger.debug(f"All messages processed for {self}")
            self._scheduler_status = 0


class DefaultMailboxFactory:
    def __call__(self) -> Mailbox:
        return DefaultMailbox()


class ActorProcess(Process):
    def __init__(self, mailbox) -> None:
        self.dead = False
        self._mailbox = mailbox

    def send_user_message(self, pid: PID, message: Any):
        self._mailbox.post_user_message(message)

    def send_system_message(self, pid: PID, message: Any):
        self._mailbox.post_system_message(message)

    def stop(self, pid: PID):
        self.dead = True
        self.send_system_message(pid, "stopMessage")


class Props:
    def __init__(self, factory) -> None:
        self._spawner: SpawnerFunc = default_spawner
        self._factory = factory  # aka producer
        self._mailbox_factory = DefaultMailboxFactory()
        self._dispatcher = AsyncDispatcher()

    @property
    def factory(self):
        return self._factory

    def create_mailbox(self) -> Mailbox:
        return self._mailbox_factory()

    def spawn(
        self,
        actor_system: "ActorSystem",
        name: str,
        parent_context: "SpawnerContext" = None,
    ):
        return self._spawner(actor_system, name, self, parent_context)


class MessageInvoker(Protocol):
    def invoke_user_message(self, message: Any): ...
    def invoke_system_message(self, message: Any): ...


class ActorContext(MessageInvoker):
    def __init__(
        self, actor_system: "ActorSystem", props: Props, parent: PID | None = None
    ) -> None:
        self._behavior = None
        self._actorSystem = actor_system
        self._props: Props = props
        self._parent: PID = parent
        self._self: PID = None
        self._message = None
        self._state = 0

        self.initialize_behavior()

    def initialize_behavior(self):
        self._state = 1
        self._behavior = self._props.factory(self)

    async def invoke_user_message(self, message: Any):
        match message:
            case "Stop":
                self._state = -1
            case _:
                result = await self._behavior(self, message)
                if result is not None and issubclass(result, Behavior):
                    self._behavior = result.setup(self._actorSystem)

    def invoke_system_message(self, message: Any): ...

    def get_self(self):
        return self._self

    def spawn(self, props: Props):
        return self.spawn_named(props, str(uuid4()))

    def spawn_named(self, props: Props, name: str):
        return props.spawn(self._actorSystem, name, self)

    def send(self, pid: PID, message: Any):
        self.send_user_message(pid, message)

    def send_user_message(self, pid: PID, message: Any):
        pid.send_user_message(self._actorSystem, message)

    def __repr__(self) -> str:
        return f"ActorContext [{self._behavior.__class__.__name__}({self.get_self()})]"


class SpawnerContext(ActorContext): ...


class ProcessRegistry:
    def __init__(self, actor_system: "ActorSystem") -> None:
        self._sequence_id: int = 0
        self._actor_system = actor_system
        self._address: str = LOCAL_ADDRESS
        self._local_pids = dict()
        self._remote_handlers: List = list()

    def register_address_resolver(self, resolver):
        self._remote_handlers.append(resolver)

    def add(self, process: "Process", id: str):
        self._local_pids[id] = process
        return PID(id, self._address)

    def remove(self, pid: PID):
        process = self._local_pids.pop(pid.id)
        if process:
            process.mark_dead()

    def get(self, pid: PID | None = None):
        if not pid:
            return  # self._actor_system.dead_letter

        if pid._address is not LOCAL_ADDRESS and pid._address is not self._address:
            for handler in self._remote_handlers:
                ref = handler(pid)
                if ref:
                    return ref
            return  # self._actor_system.dead_letter

        ref = self._local_pids[pid.id]
        if not ref:
            return  # self._actor_system.dead_letter
        return ref

    def get_local(self, id: str):
        ref = self._local_pids[id]
        if not ref:
            return  # self._actor_system.dead_letter
        return ref


class Process: ...


class RootContext:
    def __init__(self, actor_system: "ActorSystem") -> None:
        self._actor_system = actor_system

    def spawn(self, props: Props):
        pid = self.spawn_named(props, str(uuid4()))
        return pid

    def spawn_named(self, props: Props, name: str):
        return props.spawn(self._actor_system, name, self)

    def get_self(self):
        return None

    def send(self, pid: PID, message: Any):
        self.send_user_message(pid, message)

    def send_user_message(self, pid: PID, message: Any):
        pid.send_user_message(self._actor_system, message)


class ActorSystem:
    def __init__(self, root_actor: Props, id: str = str(uuid4())) -> None:
        self._process_registry: ProcessRegistry = ProcessRegistry(self)
        self._root: PID = root_actor.spawn(self, id)
        self._id: str = id

    @property
    def root(self):
        return self._root

    def send(self, message):
        self._root.send_user_message(self, message)


class ReceiveBuilder:
    def __init__(self) -> None:
        pass

    def on_message(self, message_type, handler): ...


class Behavior:
    def __init__(self, context: ActorContext):
        self._context = context

    async def __call__(
        self, actor_context: ActorContext, message: Any
    ) -> "Behavior": ...

    @classmethod
    def setup(cls, context):
        return cls(context)


class TestMessage:
    def __init__(self, greet: str) -> None:
        self.greet = greet


class TestActor(Behavior):
    async def __call__(self, context: ActorContext, message: Any):
        context._actorSystem
        match message:
            case TestMessage():
                print(f"Greeting {message.greet}")
                pid = context.spawn(Props(TestActor.setup))
                context.send(pid, "Hi Child!")
                context.send(pid, "New Message")
            case "Start":
                print("Started")
                await asyncio.sleep(1)
            case _:
                print(message)
                # await asyncio.sleep(1)
                return NewActor


class NewActor(Behavior):
    async def __call__(self, context: ActorContext, message: Any):
        match message:
            case _:
                print(message)
                print("!!!")


async def main():
    props = Props(TestActor.setup)

    system = ActorSystem(props)
    system.send("Start")
    system.send(TestMessage("Mike"))
    system.send("New Behaviour")
    system.send("In New actor")
    await asyncio.sleep(2)


asyncio.run(main())
