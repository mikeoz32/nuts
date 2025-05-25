import asyncio
import nats

from example.service import CreateOrder
from nuts.proxy import AggregateProxy, RpcProxy


async def main_rpc():
    nc = await nats.connect("tls://localhost:4222")
    print(nc)

    # async def orders_handler(msg):
    #     print(msg)
    #     await nc.publish(msg.reply, b"ok")

    proxy = RpcProxy("tls://localhost:4222", "orders")
    await proxy.get_connection()

    # await nc.subscribe("orders.*", "orders_service", orders_handler)
    for i in range(100):
        # print(await nc.request("orders.get_order", b"1"))
        # print(await nc.request("orders.get_place_order", b"{'name':'new_order'}"))
        print(
            await asyncio.gather(
                *[
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                    proxy.get_order(1),
                ]
            )
        )


async def main_agg():
    aggregate = AggregateProxy("tls://localhost:4222", "orders")
    await aggregate.command(CreateOrder(order_id="order-1",order_type=1))


asyncio.run(main_agg())
