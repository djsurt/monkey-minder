"""
HERE BE DRAGONS!!!!
this was writen very last minute just to demo stuff for the presentation,
while also running into some issues in this python testing code itself,
so as a result there is a lot of jank in here.
"""

import asyncio
import cmd
import contextlib
from ipaddress import IPv4Network
from pathlib import Path
import sys
import time
import mmtester
from mmtester import NodeClusterApi, start_client, start_nodes
import IPython

_demo_tasks = list[asyncio.Task[None]]()

_WAIT_SHORTCLIENT = True
_WATCH_INTERVAL = 0.25
# _WATCH_INTERVAL = 2.0

_DO_DBGPOLL = True

_TEMP_DISABLE_WATCHES = False

async def do_watch_cluster_node(node: mmtester.NodeApi):
    targetfile = Path.cwd()/f'mm-node-statedump-{node.container.mm_id}'
    while True:
        if _WAIT_SHORTCLIENT:
            while True:
                if _TEMP_DISABLE_WATCHES:
                    break
                if await node.is_running():
                    async with start_client(node) as client:
                        state = await client._get_dbgstatedump()
                else:
                    state = None
                if state is not None:
                    targetfile.write_text(state, encoding='utf-8')
                else:
                    targetfile.write_text("<node is dead>", encoding='utf-8')
                    break
                await asyncio.sleep(_WATCH_INTERVAL)
        else:  # long client
            while True:
                if _TEMP_DISABLE_WATCHES:
                    break
                async with start_client(node) as client:
                    if await node.is_running():
                        state = await client._get_dbgstatedump()
                    else:
                        break
                    if state is not None:
                        targetfile.write_text(state, encoding='utf-8')
                    else:
                        targetfile.write_text("<node is dead>", encoding='utf-8')
                        break
                    await asyncio.sleep(_WATCH_INTERVAL)
        targetfile.write_text("<node is dead>", encoding='utf-8')
        while True:
            if _TEMP_DISABLE_WATCHES:
                break
            if await node.is_running():
                break
            await asyncio.sleep(_WATCH_INTERVAL)
        while _TEMP_DISABLE_WATCHES:
            targetfile.write_text("<debug view disabled>", encoding='utf-8')
            await asyncio.sleep(5.0)

_demo_fns = dict()
def demo_func(size):
    def wrapper(func):
        _demo_fns[func.__name__] = size, func
    return wrapper

def demo_main():
    import faulthandler
    faulthandler.enable()
    import logging
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    asyncio.run(_demo_amain())

# async def wtf():
#     while True:
#         print('wtf')
#         await asyncio.sleep(0.1)

async def _demo_amain():
    await mmtester.setup(mmtester.Config(image_name='monkey-minder-server', use_docker_network=False, graceful_stop_containers=False, network_subnet=IPv4Network('10.10.0.0/16'), network_name='monkey-minder-test-network', 
    has_iptables=False))

    cluster_size, func = _demo_fns[sys.argv[1]]

    async with start_nodes(cluster_size) as cluster:
        async with asyncio.TaskGroup() as _demo_tg:
            # _demo_tg.create_task(wtf())
            for n in range(1, cluster_size+1):
                (Path.cwd()/f'mm-node-statedump-{n}').write_text('')
            await asyncio.sleep(1.0)
            if _DO_DBGPOLL:
                for __node in cluster:
                    _demo_tg.create_task(do_watch_cluster_node(__node))
            await func(cluster)
            for task in _demo_tg._tasks:
                task.cancel()

@demo_func(3)
async def leader_election(cluster: NodeClusterApi):
    leader, followers = await cluster.wait_for_election()
    await asyncio.sleep(6.0)
    print('killing initial leader...')
    await leader.kill()
    await asyncio.sleep(6.0)
    leader2, followers2 = await cluster.wait_for_election()
    await asyncio.sleep(6.0)
    print('killing subsequent leader...')
    await leader2.kill()
    await asyncio.sleep(10)
    print('reviving one node')
    await leader2.revive(wait=False)
    await asyncio.sleep(10)
    print('done!')

@demo_func(3)
async def leader_writes(cluster: NodeClusterApi):
    leader, followers = await cluster.wait_for_election()
    await asyncio.sleep(6.0)
    async with start_client(leader) as c:
        await c.create('/foo', '0')
        for i in range(1, 32+1):
            await c.set_data('/foo', f'{i}', -1)
            await asyncio.sleep(0.1)
    await asyncio.sleep(10.0)
    print('done!')

@demo_func(3)
async def follower_writes(cluster: NodeClusterApi):
    leader, followers = await cluster.wait_for_election()
    await asyncio.sleep(2.0)
    async with start_client(followers[0]) as c:
        print(f'sending writes to #{followers[0].container.mm_id} (follower)')
        await c.create('/foo', '0')
        for i in range(1, 32+1):
            await c.set_data('/foo', f'{i}', -1)
    await asyncio.sleep(10.0)
    print('done!')

@demo_func(3)
async def leader_death_recover(cluster: NodeClusterApi):
    leader, followers = await cluster.wait_for_election()
    await asyncio.sleep(2.0)
    async with start_client(followers[0]) as c:
        print(f'sending writes to #{followers[0].container.mm_id} (follower)')
        for (k, v) in [('/a','1'), ('/b','2'), ('/c','3'), ('/d','4')]:
            await c.create(k, v)
    await leader.kill()
    await asyncio.sleep(8.0)
    await leader.revive()
    await asyncio.sleep(10.0)
    print('done!')

@demo_func(5)
async def many_many_many_reads(cluster: NodeClusterApi):
    CLUSTER_SIZE = len(list(cluster))
    global _TEMP_DISABLE_WATCHES
    _TEMP_DISABLE_WATCHES = True
    leader, followers = await cluster.wait_for_election()
    async with start_client(followers[0]) as c:
        print(f'sending writes to #{followers[0].container.mm_id} (follower)')
        for (k, v) in [('/a','1'), ('/b','2'), ('/c','3'), ('/d','4')]:
            await c.create(k, v)
    N_PER_SERVER = 25
    N_READS_PER_CLIENT = 500
    async with contextlib.AsyncExitStack() as stack:
        clients = []
        print(f'connecting {N_PER_SERVER} clients to each node ({N_PER_SERVER * CLUSTER_SIZE} total)')
        for node in cluster:
            for _ in range(N_PER_SERVER):
                clients.append(await stack.enter_async_context(start_client(node)))
        start_reads_event = asyncio.Event()
        async def per_client_logic(client):
            # futures = []
            await start_reads_event.wait()
            # for _ in range(N_READS_PER_CLIENT//4):
            #     for k in ['/a', '/b', '/c', '/d']:
            # for _ in range(N_READS_PER_CLIENT):
            #     futures.append(client.get_data(k))
            # await asyncio.gather(*[client.get_data('/a') for _ in range(N_READS_PER_CLIENT)])
            for _ in range(N_READS_PER_CLIENT - 1):
                await client._outgoing.put(mmtester.ClientRequest(kind=mmtester.protos.GETDATA, id=client._next_num(), path='/a'))
            await client.get_data('/a')
        async with asyncio.TaskGroup() as tg:
            for c in clients:
                tg.create_task(per_client_logic(c))
            print(f'requesting {N_READS_PER_CLIENT} reads from each client')
            time_start = time.perf_counter_ns()
            start_reads_event.set()
        time_end = time.perf_counter_ns()
        print('done!')
        total_reads_done = (N_PER_SERVER * CLUSTER_SIZE) * N_READS_PER_CLIENT
        time_delta = time_end - time_start
        nanos_per_read = time_delta // total_reads_done
        print(f'served {total_reads_done} reads across {CLUSTER_SIZE} nodes in {time_delta // 1000000}ms (approx. {nanos_per_read / 1000000:0.04f}ms per read)')
        await asyncio.sleep(999)

