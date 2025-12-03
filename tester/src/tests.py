import asyncio
from ipaddress import IPv4Address, IPv4Network
import unittest

import mmtester
from mmtester import start_client, start_nodes

class TestAssignment3(unittest.IsolatedAsyncioTestCase):
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

    async def test_1(self):
        async with start_nodes(3) as cluster:
            leader, followers = await cluster.wait_for_election()

            async with start_client(leader) as c:
                await c.create('/foo', 'v1')
                self.assertEqual((await c.get_data('/foo')).data, 'v1')
                await c.set_data('/foo', 'v2', -1)
                self.assertEqual((await c.get_data('/foo')).data, 'v2')
                await c.set_data('/foo', 'v3', -1)
                self.assertEqual((await c.get_data('/foo')).data, 'v3')

    async def test_2(self):
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


def main():
    _ = unittest.main(module='tests')
