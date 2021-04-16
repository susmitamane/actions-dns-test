
import socket
import time

import zmq
from zmq.asyncio import Context, Poller
import asyncio

from async_timeout import timeout


ctx = Context.instance()


async def receiver(host, port):
    """receive messages with polling"""
    pull = ctx.socket(zmq.PULL)
    url = f'tcp://{host}:{port}'
    pull.connect(url)
    poller = Poller()
    poller.register(pull, zmq.POLLIN)
    async with timeout(10):
        while True:
            try:
                events = await poller.poll()
            except:
                # swallow "task exception was never retrieved" errors
                break
            if pull in dict(events):
                print("recving", events)
                msg = await pull.recv_multipart()
                print('recvd', msg)
                if msg == [b'END']:
                    break
    print('done')


async def sender(port):
    """send a message every second"""
    tic = time.time()
    push = ctx.socket(zmq.PUSH)
    try:
        url = f'tcp://*:{port}'
        push.bind(url)
        for _ in range(3):
            print("sending")
            await push.send_multipart([str(time.time() - tic).encode('ascii')])
            await asyncio.sleep(1)
        await push.send_multipart(['END'.encode('ascii')])
    finally:
        push.close()

port = 43087
for host in [
    '127.0.0.1',
    socket.gethostname(),
    socket.getfqdn(),
]:
    print('\n\n\n')
    print(f'# {host}:{port}')
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait(
            [
                receiver(host, port),
                sender(port),
            ]
        )
    )
    print('\n\n\n')
