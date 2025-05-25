import contextlib
from dataclasses import dataclass


from nuts.nuts import Nuts
from nuts.server.nsgi import NsgiServer




@contextlib.asynccontextmanager
async def ls():
    yield {"a": "b"}


app = Nuts("orders", lifespan=ls)


async def get_order(id, pool):
    return {}


app.add_method("get_order", get_order)


@dataclass
class CreateOrder:
    order_id: str
    order_type: int


if __name__ == "__main__":
    NsgiServer("tls://localhost:4222").run(app)
