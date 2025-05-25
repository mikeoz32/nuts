import asyncio

import nats


async def main():
    nc = await nats.connect("tls://localhost:4222")

    js = nc.jetstream()

    bk = await js.create_key_value(bucket="test_aggregate")

    w = await bk.put("node", b"a")


asyncio.run(main())
