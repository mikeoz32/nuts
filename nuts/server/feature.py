class ServerFeature:
    """
    Feature supported by server like rpc or tasks
    """

    async def startup(self): ...

    async def shutdown(self): ...

    async def on_tick(self, counter): ... 
