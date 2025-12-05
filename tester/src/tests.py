import asyncio
from contextlib import asynccontextmanager
from ipaddress import IPv4Address, IPv4Network
import unittest

import mmtester
from mmtester import start_client, start_nodes

def test_timeout(timeout):
    def wrapper(test_method):
        async def wrapped(self: 'TestMonkeyMinder'):
            try:
                async with asyncio.timeout(timeout):
                    return await test_method(self)
            except asyncio.TimeoutError:
                self.fail('test timed out')
        return wrapped
    return wrapper

class TestMonkeyMinder(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await mmtester.setup(mmtester.Config(
            image_name='monkey-minder-server',
            # host_ip=IPv4Address('0.0.0.0'),
            network_name='monkey-minder-test-network',
            network_subnet=IPv4Network('10.10.0.0/16'),
            use_docker_network=False,
            graceful_stop_containers=False,
        ))

    async def asyncTearDown(self) -> None:
        await mmtester.cleanup()

    @test_timeout(20.0)
    async def test_1(self):
        print('test_1')
        async with start_nodes(3) as cluster:
            leader, followers = await cluster.wait_for_election()

            async with start_client(leader) as c:
                await c.create('/foo', 'v1')
                self.assertEqual((await c.get_data('/foo')).data, 'v1')
                await c.set_data('/foo', 'v2', -1)
                self.assertEqual((await c.get_data('/foo')).data, 'v2')
                await c.set_data('/foo', 'v3', -1)
                self.assertEqual((await c.get_data('/foo')).data, 'v3')

    @test_timeout(20.0)
    async def test_2(self):
        print('test_2')
        async with start_nodes(3) as cluster:
            leader, followers = await cluster.wait_for_election()

            async with start_client(leader) as c:
                await c.create('/bar', 'X')
            async with start_client(leader) as c1, start_client(leader) as c2:
                await asyncio.gather(
                    c1.set_data('/bar', 'A', -1),
                    c2.set_data('/bar', 'B', -1),
                )
            # give it some time to propagate
            await asyncio.sleep(2.0)
            responses = set[str]()
            for n in cluster:
                async with start_client(n) as c:
                    responses.add((await c.get_data('/bar')).data)
            self.assertEqual(len(responses), 1, f"expected uniform data across nodes! (got {responses})")
            response = next(iter(responses))
            self.assertIn(response, {'A', 'B'})

    @test_timeout(20.0)
    async def test_3(self):
        print('test_3')
        async with start_nodes(3) as cluster, asyncio.TaskGroup() as tg:
            leader, followers = await cluster.wait_for_election()
            async def t1():
                async with start_client(leader) as c:
                    await c.create('/m', '1')
                    await asyncio.sleep(2.0)
                    await c.set_data('/m', '2', -1)
            async def t2():
                async with start_client(followers[0]) as c:
                    while True:
                        data = await c.get_data('/m')
                        if data.data == '2':
                            break
                        await asyncio.sleep(0.1)
                    for _ in range(20):
                        data = await c.get_data('/m')
                        if data.data == '1':
                            self.fail()
                        await asyncio.sleep(0.1)
            tg.create_task(t1())
            tg.create_task(t2())

    # async def test_partitioned_leader_causes_election(self):
    #     async with start_nodes(5) as cluster:
    #         leader, followers = await cluster.wait_for_election()
    #         # parition leader from all other nodes
    #         await asyncio.gather(*[leader.create_partition(n) for n in followers])
    #         # ... then some time passes ...
    #         await asyncio.sleep(5.0)
    #         # old leader hasn't ceased being leader under partition
    #         self.assertTrue(await leader.is_leader())
    #         # but, someone else will have become leader since then
    #         self.assertTrue(any(await asyncio.gather(*[n.is_leader() for n in followers])))

    @test_timeout(20.0)
    async def test_killed_leader_causes_election(self):
        print('test_killed_leader_causes_election')
        async with start_nodes(5) as cluster:
            leader, followers = await cluster.wait_for_election()
            # parition leader from all other nodes
            await leader.kill()
            # ... then some time passes ...
            await asyncio.sleep(4.0)
            # and someone else will have become leader since then
            self.assertTrue(any(await asyncio.gather(*[n.is_leader() for n in followers])))

    @test_timeout(30.0)
    async def test_restarted_node_unblocks_election(self):
        print('test_restarted_node_unblocks_election')
        async with start_nodes(5) as cluster:
            # initial election happens
            leader, followers = await cluster.wait_for_election()
            await leader.kill()
            await followers[0].kill()
            await followers[1].kill()
            await asyncio.sleep(4.0)
            self.assertFalse(any(await asyncio.gather(*[n.is_leader() for n in followers])))
            await followers[0].revive()
            await asyncio.sleep(4.0)
            self.assertTrue(any(await asyncio.gather(*[n.is_leader() for n in followers])))

def main():
    _ = unittest.main(module='tests')
