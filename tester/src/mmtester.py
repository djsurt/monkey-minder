__all__ = ['setup', 'cleanup', 'start_nodes', 'start_client']

import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from grpc import ChannelConnectivity
import io
from ipaddress import IPv4Address, IPv4Network
import itertools
import logging
import tarfile
from types import TracebackType
from typing import TYPE_CHECKING, Any, Awaitable, Coroutine, Generator, Mapping, NamedTuple, Self
from collections.abc import AsyncIterator
import aiodocker
from aiodocker.execs import Exec
import aiohttp
import grpc
from yarl import URL
from proto.monkeyminder_service_pb2_grpc import MonkeyMinderService, MonkeyMinderServiceStub
import proto.monkeyminder_service_pb2 as protos
from proto.monkeyminder_service_pb2 import ClientRequest, ServerResponse

if TYPE_CHECKING:
    from aiodocker.containers import DockerContainer
    from aiodocker.networks import DockerNetwork


logger = logging.getLogger(__name__)


async def _noop() -> None:
    pass


@dataclass(frozen=True, kw_only=True, match_args=False)
class Config:
    image_name: str = 'kvs:2.0'
    """name of the docker image to use"""
    network_name: str = 'kv_subnet'
    """name of the docker network to use"""
    network_subnet: IPv4Network = IPv4Network('10.10.0.0/16')
    """subnet of the docker network"""
    network_subnet_skipfirst: int = 2
    """don't use the first n ips of the subnet"""
    base_host_port: int = 13800
    """
    created containers' mapped ports will be assigned starting from this number and counting up
    (e.g. for a value of 123, the first container created will map to 123, the second to 124, etc.)
    """
    host_ip: IPv4Address = IPv4Address('127.0.0.1')
    graceful_stop_containers: bool = False
    """True = stop containers with stop, False = stop containers with kill"""
    alivetest_retry_delay: float = 0.1
    """how many seconds to wait between pings while waiting for a container to start"""
    use_docker_network: bool = True

    def _str_for_container_compare(self) -> str:
        return f'image:{self.network_name!r} network:{self.network_name!r} subnet:{self.network_subnet} skip {self.network_subnet_skipfirst} base port:{self.base_host_port} host ip:{self.host_ip} use docker network:{self.use_docker_network}'


CONTAINER_LABEL_CLIENTNUM = 'asg3tester.clientnum'
CONTAINER_LABEL_COMPARESTR = 'asg3tester.comparestr'


config_var: ContextVar[Config] = ContextVar('config')
docker_client_var: ContextVar[aiodocker.Docker] = ContextVar('docker_client')
kvs_image_var: ContextVar[Mapping[str, Any]] = ContextVar('kvs_image')
kvs_network_var: ContextVar['DockerNetwork'] = ContextVar('kvs_network')
http_session_var: ContextVar[aiohttp.ClientSession] = ContextVar('http_session')
nodecontainer_cache_var: ContextVar[dict[int, 'NodeContainer']] = ContextVar('nodecontainer_cache')

async def setup(config: Config | None = None) -> None:
    if config is None:
        config = Config()

    _ = config_var.set(config)
    _ = http_session_var.set(aiohttp.ClientSession())
    _ = docker_client_var.set(docker_client := aiodocker.Docker())
    _ = kvs_image_var.set(kvs_image := await docker_client.images.inspect(config.image_name))
    _ = kvs_network_var.set(kvs_network := await docker_client.networks.get(config.network_name))
    _ = nodecontainer_cache_var.set(dict())

    for container in await docker_client.containers.list(
        all=True,
        # filters={'network': [kvs_network.id]},
    ):
        # print(f'{container=}')
        container_info = await container.show()
        clientnum = container_info['Config']['Labels'].get(CONTAINER_LABEL_CLIENTNUM, None)
        comparestr = container_info['Config']['Labels'].get(CONTAINER_LABEL_COMPARESTR, None)
        if clientnum is None:
            if container_info['State']['Status'] == 'running':
                logger.error(f'''a container that isn't managed by asg3tester is running on the network, will likely cause issues (has id: {container_info['Id']})''')
        else:
            if (
                # if the image has been rebuilt since that container was made then we can't reuse it
                (container_info['Image'] != kvs_image['Id'])
                # if the config has changed since the container was made then we can't reuse it
                or (comparestr != config._str_for_container_compare())
            ):
                logger.info(f'deleting outdated asg3tester managed container {container.id!r}')
                # await container.kill()
                await container.delete(force=True)
            else:
                logger.debug(f'reusing container #{clientnum} ({container.id!r})')
                if container_info['State']['Status'] in {'running'}:
                    await container.kill()
                await NodeContainer._register_container(num=int(clientnum), container=container)

async def cleanup() -> None:
    docker_client = docker_client_var.get(None)
    http_session = http_session_var.get(None)
    if docker_client is not None:
        await docker_client.close()
    if http_session is not None:
        await http_session.close()


class NodeContainer:
    _num: int
    mm_id: int
    host_ip: IPv4Address
    host_port: int
    subnet_ip: IPv4Address
    subnet_port: int
    container: 'DockerContainer'

    @property
    def address(self) -> str:
        return f'{self.subnet_ip}:{self.subnet_port}'

    @classmethod
    async def _create(cls, num: int, container: 'DockerContainer | None' = None) -> 'NodeContainer':
        config = config_var.get()
        node = NodeContainer()
        node._num = num
        node.mm_id = num + 1
        node.host_ip = config.host_ip
        node.host_port = config.base_host_port + num
        # node.subnet_port = 9000 + node.mm_id
        if config.use_docker_network:
            node.subnet_ip = config.network_subnet[num + config.network_subnet_skipfirst]
            node.subnet_port = 1234
        else:
            node.subnet_ip = node.host_ip
            node.subnet_port = node.host_port
        if container is not None:
            node.container = container
        else:
            node.container = await node._make_container_for()
        return node

    async def _make_container_for(self) -> 'DockerContainer':
        docker_client = docker_client_var.get()
        kvs_image = kvs_image_var.get()
        kvs_network = kvs_network_var.get()
        config = config_var.get()
        container: 'DockerContainer' = await docker_client.containers.create(
            name=f'mm-test-node-{self._num}',
            config={
                'Cmd': ['/bin/server', '--id', f'{self.mm_id}'],
                'Env': [
                    'GRPC_GO_LOG_VERBOSITY_LEVEL=99',
                    'GRPC_GO_LOG_SEVERITY_LEVEL=info',
                ],
                'Image': kvs_image['Id'],
                'ExposedPorts': { f'{self.subnet_port}': {} },
                'AttachStdout': True, 'AttachStderr': True,
                'HostConfig': {
                    'NetworkMode': config.network_name if config.use_docker_network else 'host',
                    'PublishAllPorts': False,
                    'PortBindings': {
                        f'{self.subnet_port}/{protocol}': [
                            {
                                'HostIp': f'{self.host_ip}',
                                'HostPort': f'{self.host_port}',
                            },
                        ]
                        for protocol in ['tcp', 'udp']
                    } if config.use_docker_network else {},
                },
                'Labels': {
                    CONTAINER_LABEL_CLIENTNUM: f'{self._num}',
                    CONTAINER_LABEL_COMPARESTR: config._str_for_container_compare(),
                },
            }
        )
        # container.get_archive
        # await kvs_network.connect(config={
        #     'Container': container.id,
        #     'EndpointConfig': {
        #         'IPAddress': f'{self.subnet_ip}',
        #         # 'IPAMConfig': {
        #         #     'IPv4Address': f'{self.subnet_ip}',
        #         # },
        #     },
        # })
        return container

    @classmethod
    async def _get_or_create(cls, num: int) -> 'NodeContainer':
        nodes = nodecontainer_cache_var.get()
        match nodes.get(num):
            case None:
                node = nodes[num] = await NodeContainer._create(num=num, container=None)
                return node
            case node:
                return node

    @classmethod
    async def _register_container(cls, num: int, container: 'DockerContainer') -> None:
        nodes = nodecontainer_cache_var.get()
        assert num not in nodes
        nodes[num] = await NodeContainer._create(num=num, container=container)

    async def _start(self):
        await self.container.start()
        await self.__wait_for_start()

    async def __wait_for_start(self) -> None:
        retry_delay = config_var.get().alivetest_retry_delay
        num_tries = 0
        while True:
            try:
                async with grpc.aio.insecure_channel(f'{self.host_ip}:{self.host_port}') as channel:
                    while True:
                        match channel.get_state(try_to_connect=True):
                            case grpc.ChannelConnectivity.READY:
                                return
                            case grpc.ChannelConnectivity.TRANSIENT_FAILURE | grpc.ChannelConnectivity.SHUTDOWN:
                                break
                            case grpc.ChannelConnectivity.CONNECTING | grpc.ChannelConnectivity.IDLE:
                                pass
                        await asyncio.sleep(retry_delay)
                        num_tries += 1
            except Exception as ex:
                print(f'otherwise-unhandled error ignored in wait-for-start loop: {ex}')
            await asyncio.sleep(retry_delay)
            num_tries += 1

    def _stop(self) -> Coroutine[Any, Any, None]:
        return self.__stop()

    async def __stop(self) -> None:
        config = config_var.get()
        if (await self.container.show())['State']['Running']:
            if config.graceful_stop_containers:
                await self.container.stop()
            else:
                await self.container.kill()

    @classmethod
    @asynccontextmanager
    async def start_group_of_n(cls, n_nodes: int, start: bool) -> AsyncIterator[list['NodeApi']]:
        nodes = list[NodeApi]()

        try:
            for n in range(n_nodes):
                container = await cls._get_or_create(n)
                await container.__stop()
                nodes.append(NodeApi(container))
            cluster = _setup_cluster_config(nodes)
            for node in nodes:
                _ = await node.container.container.put_archive('/', cluster)
            if start:
                for node in nodes:
                    await node.container._start()
            yield nodes
        finally:
            for node in nodes:
                await node.kill()

    async def _exec(self, *cmd: str, privileged: bool = False) -> Exec:
        execute: Exec = await self.container.exec(cmd=cmd, privileged=privileged)
        async with execute.start(detach=False) as stream:
            out = await stream.read_out()
        return execute



class NodeApi:
    container: NodeContainer

    @property
    def address(self) -> str:
        return self.container.address

    def __init__(self, container: NodeContainer) -> None:
        self.container = container

    async def kill(self) -> None:
        await self.container._stop()

    # TODO give those params actual names
    def _iptables_exec(self, other: 'NodeApi', foo: str, bar: str) -> Awaitable[Exec]:
        return self.container._exec('iptables', foo, bar, '--source', str(other.container.subnet_ip), '--jump', 'DROP', privileged=True)

    async def create_partition(self, other: 'NodeApi') -> None:
        # TODO check to make sure commands actually ran successfully
        await self._iptables_exec(other, '--append', 'INPUT')
        await self._iptables_exec(other, '--append', 'OUTPUT')
        await other._iptables_exec(self, '--append', 'INPUT')
        await other._iptables_exec(self, '--append', 'OUTPUT')

    async def heal_partition(self, other: 'NodeApi') -> None:
        # TODO check to make sure commands actually ran successfully
        await self._iptables_exec(other, '--delete', 'INPUT')
        await self._iptables_exec(other, '--delete', 'OUTPUT')
        await other._iptables_exec(self, '--delete', 'INPUT')
        await other._iptables_exec(self, '--delete', 'OUTPUT')

    async def is_leader(self) -> bool:
        async with start_client(self) as client:
            return await client.is_leader()

class _ClientGetDataResponse(NamedTuple):
    data: str
    version: int

class ClientApi:
    _num: int
    _outgoing: asyncio.Queue[ClientRequest]
    _response_events: dict[int, asyncio.Event]
    _response_datas: dict[int, ServerResponse]
    _session: 'grpc.aio.StreamStreamCall[ClientRequest, ServerResponse]'

    def __init__(self, channel: grpc.aio.Channel, /) -> None:
        self._num = 0
        self._outgoing = asyncio.Queue[ClientRequest]()
        self._response_events = dict()
        self._response_datas = dict()
        stub = MonkeyMinderServiceStub(channel)
        self._session = stub.Session()
        self._tasks = [
            asyncio.create_task(self._send_loop()),
            asyncio.create_task(self._recv_loop()),
        ]

    async def _send_loop(self):
        while True:
            msg = await self._outgoing.get()
            # print(f'outgoing {msg=}')
            await self._session.write(msg)
            self._outgoing.task_done()

    async def _recv_loop(self):
        while True:
            try:
                msg = await self._session.read()
            except grpc.aio.AioRpcError as ex:
                if ex.code() == grpc.StatusCode.UNAVAILABLE:
                    continue
                print(f'client died! with message {ex=}')
                break
            else:
                # print(f'got {msg=}')
                match msg:
                    case m if m == grpc.aio.EOF:
                        break
                    case ServerResponse() as resp:
                        if resp.id in self._response_events:
                            self._response_datas[resp.id] = resp
                            self._response_events[resp.id].set()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        for task in self._tasks:
            _ = task.cancel()

    def _next_num(self) -> int:
        self._num += 1
        return self._num

    async def _api(self, request: ClientRequest, /) -> ServerResponse:
        event = asyncio.Event()
        self._response_events[request.id] = event
        await self._outgoing.put(request)
        _ = await event.wait()
        resp = self._response_datas[request.id]
        del self._response_events[request.id]
        del self._response_datas[request.id]
        return resp

    async def create(self, path: str, data: str) -> str:
        return (await self._api(ClientRequest(kind=protos.CREATE, id=self._next_num(), path=path, data=data))).data

    async def delete(self, path: str, version: int) -> None:
        _ = await self._api(ClientRequest(kind=protos.DELETE, id=self._next_num(), path=path, version=version))

    async def get_data(self, path: str) -> _ClientGetDataResponse:
        resp = await self._api(ClientRequest(kind=protos.GETDATA, id=self._next_num(), path=path))
        return _ClientGetDataResponse(resp.data, resp.version)

    async def set_data(self, path: str, data: str, version: int) -> None:
        _ = await self._api(ClientRequest(kind=protos.SETDATA, id=self._next_num(), path=path, data=data, version=version))

    async def get_children(self, path: str) -> list[str]:
        return list((await self._api(ClientRequest(kind=protos.GETCHILDREN, id=self._next_num(), path=path))).children)

    async def _get_leaderinfo(self) -> tuple[int, bool]:
        resp = await self._api(ClientRequest(kind=protos.INTERNAL_LEADERCHECK, id=self._next_num()))
        return resp.version, resp.internal_isleader

    async def is_leader(self) -> bool:
        return (await self._get_leaderinfo())[1]

def start_nodes(n: int, /, *, start=True) -> AbstractAsyncContextManager[list[NodeApi]]:
    return NodeContainer.start_group_of_n(n, start)

def _setup_cluster_config(nodes: list[NodeApi]) -> bytes:
    cluster_config_text = '\n'.join([
        # f"{n.container.mm_id} {n.container.name}:{n.container.subnet_port}"
        f"{n.container.mm_id} {n.container.subnet_ip}:{n.container.subnet_port}"
        for n in nodes
    ]) + "\n"
    cluster_config_data = cluster_config_text.encode('utf-8')

    f = io.BytesIO()
    ff = io.BytesIO()
    _ = ff.write(cluster_config_data)
    _ = ff.seek(0)
    with tarfile.open(fileobj=f, mode='w') as tf:
        info = tarfile.TarInfo('cluster.conf')
        info.size = len(cluster_config_data)
        tf.addfile(info, ff)
    del ff
    tardata = f.getvalue()
    return tardata

@asynccontextmanager
async def start_client(server: NodeApi, /) -> AsyncIterator[ClientApi]:
    cfg = config_var.get()
    async with grpc.aio.insecure_channel(f'{server.container.host_ip}:{server.container.host_port}') as channel:
        try:
            async with ClientApi(channel) as client:
                yield client
        finally:
            pass
