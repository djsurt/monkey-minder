import asyncio
import cmd
from ipaddress import IPv4Network
from pathlib import Path
import sys
import mmtester
from mmtester import NodeClusterApi, start_client, start_nodes
import IPython

_demo_tasks = list[asyncio.Task[None]]()

_WAIT_SHORTCLIENT = False
_WATCH_INTERVAL = 0.25
# _WATCH_INTERVAL = 2.0

_DO_DBGPOLL = True

async def do_watch_cluster_node(node: mmtester.NodeApi):
    targetfile = Path.cwd()/f'mm-node-statedump-{node.container.mm_id}'
    while True:
        if _WAIT_SHORTCLIENT:
            while True:
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
            if await node.is_running():
                break
            await asyncio.sleep(_WATCH_INTERVAL)

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
    async with start_client(followers[0]) as c, start_client(leader) as c2:
        print(f'sending writes to #{followers[0].container.mm_id} (follower)')
        print(f'sending reads  to #{leader.container.mm_id} (leader)')
        await c.create('/foo', '0')
        for i in range(1, 32+1):
            await c.set_data('/foo', f'{i}', -1)
            await asyncio.sleep(2.0)
            data = await c2.get_data('/foo')
            print(f'{data=}')
    await asyncio.sleep(10.0)
    print('done!')

