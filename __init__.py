import asyncio
from .mocks import MockConnection, MockChannel, MockMessage

@asyncio.coroutine
def connect(host='localhost',
            port=5672,
            username='guest', password='guest',
            virtual_host='/', *,
            loop=None, **kwargs):
    loop = asyncio.get_event_loop() if loop is None else loop
    return MockConnection(loop)


@asyncio.coroutine
def connect_and_open_channel(host='localhost',
                             port=5672,
                             username='guest', password='guest',
                             virtual_host='/', *,
                             loop=None, **kwargs):
    connection = yield from connect(host, port, username, password, virtual_host, loop=loop, **kwargs)
    channel = yield from connection.open_channel()
    return connection, channel