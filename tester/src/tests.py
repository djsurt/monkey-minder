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
        ))

    async def asyncTearDown(self) -> None:
        await mmtester.cleanup()

    async def test_get_2nodes(self):
        async with start_nodes(3) as (n1, n2, n3), start_client(n1) as c1:
            _ = await c1.create('/a', 'b')

def main():
    _ = unittest.main(module='tests')
