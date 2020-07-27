import cudf
import ucp
from distributed import get_worker

from distributed.comm.ucx import UCXListener
from distributed.comm.ucx import UCXConnector
from distributed.comm.addressing import parse_host_port
from distributed.protocol.serialize import to_serialize
import concurrent.futures
from concurrent.futures import CancelledError
import asyncio
import traceback

from dask.distributed import default_client

serde = ("cuda", "dask", "pickle", "error")

async def route_message(msg):
    worker = get_worker()
    if msg.metadata["add_to_specific_cache"] == "true":
        graph = worker.query_graphs[int(msg.metadata["query_id"])]
        cache = graph.get_kernel_output_cache(
            int(msg.metadata["kernel_id"]),
            msg.metadata["cache_id"]
        )
        cache.add_to_cache(msg.data)
    else:
        cache = worker.input_cache
        if(msg.data is None):
            msg.data = cudf.DataFrame()
        cache.add_to_cache_with_meta(msg.data, msg.metadata)


# async def run_comm_thread():  # doctest: +SKIP
#    dask_worker = get_worker()
#    import asyncio
#    while True:
#        df, metadata = dask_worker.output_cache.pull_from_cache()
#        await UCX.get().send(BlazingMessage(df, metadata))
#        await asyncio.sleep(1)

class Communicator():  # doctest: +SKIP

    def __init__(self):
        self.dask_worker = get_worker()
        # create a single UCX instance for the lifetime of this worker
        if not hasattr(self.dask_worker, 'ucx'):
            # create a single UCX instance for the lifetime of this worker
            self.dask_worker.ucx = UCX()

        self.ucx = self.dask_worker.ucx
        self.stoprequest = asyncio.Event()

    async def get_ucx_address(self):
        self.ucx_address = await self.ucx.get_listener(route_message)

        return self.ucx_address

    async def create_endpoints(self, addr_map):
        await self.ucx.init_handlers(addr_map)

    async def start(self):
        async def work():
            while not self.stoprequest.is_set():
                # the latency is intentional to not overwhelm the asyncio loop with (blocking) cache polls
                await asyncio.sleep(0.001)
                have_data = self.dask_worker.output_cache.has_next_now()

                if have_data:
                    df, metadata = self.dask_worker.output_cache.pull_from_cache()
                    if metadata["add_to_specific_cache"] == "false" and len(df) == 0:
                        df = None
                    await self.ucx.send(BlazingMessage(metadata, df))

            if self.dask_worker.output_cache.has_next_now():
                raise RuntimeError('Message left in queue after graph completion.')

        await work()

    async def stop(self, worker_task):
        self.stoprequest.set()
        await worker_task

class BlazingMessage:
    def __init__(self, metadata, data=None):
        self.metadata = metadata
        self.data = data

    def is_valid(self):
        return ("query_id" in self.metadata and
                "cache_id" in self.metadata and
                "worker_ids" in self.metadata and
                len(self.metadata["worker_ids"]) > 0 and
                self.data is not None)


class UCX:
    """
    UCX context to encapsulate all interactions with the
    UCX-py API and guarantee only a single listener & endpoints are
    created by cuML on a single process.
    """

    def __init__(self):

        self.callback = None
        self._endpoints = {}
        self._listener = None
        self.received = 0
        self.sent = 0
        self.lock = asyncio.Lock()
        self.ucx_addresses = None
        self.write_queues = dict()
        self.worker_tasks = dict()

    async def get_listener(self, callback):
        if self._listener is not None:
            if callback != self.callback:
                raise RuntimeError("Updating the UCX listener callback is not implemented.")
            return self._listener.address
        self.callback = callback
        return await self.start_listener()

    async def init_handlers(self, ucx_addresses):
        self.ucx_addresses = ucx_addresses
        eps = []
        for address in self.ucx_addresses.values():
            ep = await self.get_endpoint(address)

    @staticmethod
    def get_ucp_worker():
        return ucp.core._ctx.worker

    async def start_listener(self):

        async def handle_comm(comm):
            try:
                while not comm.closed():
                    msg = await comm.read()

                    msg = BlazingMessage(**{k: v.deserialize()
                                            for k, v in msg.items()})
                    self.received += 1
                    await self.callback(msg)
            except CancelledError:
                pass
            except Exception as e:
                raise

        ip, port = parse_host_port(get_worker().address)
        self._listener = await UCXListener(ip, handle_comm)
        await self._listener.start()
        return self._listener.address

    def listener_port(self):
        return self._listener.port

    async def worker(self, queue, ep):
        while True:
            msg = await queue.get()
            await ep.write(msg=msg, serializers=serde)

    async def _create_endpoint(self, addr):
        ep = await UCXConnector().connect(addr)
        self._endpoints[addr] = ep
        queue = asyncio.Queue()
        self.write_queues[addr] = queue
        self.worker_tasks[addr] = asyncio.create_task(self.worker(queue, ep))
        return ep

    async def get_endpoint(self, addr):
        if addr not in self._endpoints:
            ep = await self._create_endpoint(addr)
        else:
            ep = self._endpoints[addr]

        return ep

    async def send(self, blazing_msg):
        """
        Send a BlazingMessage to the workers specified in `worker_ids`
        field of metadata
        """
        local_dask_addr = self.ucx_addresses[get_worker().address]
        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
            addr = self.ucx_addresses[dask_addr]
            ep = await self.get_endpoint(addr)

            to_ser = {"metadata": to_serialize(blazing_msg.metadata)}

            if blazing_msg.data is not None:
                to_ser["data"] = to_serialize(blazing_msg.data)

            self.write_queues[addr].put_nowait(to_ser)
            self.sent += 1

    def abort_endpoints(self):
        for addr, ep in self._endpoints.items():
            if not ep.closed():
                ep.abort()
            del ep
        self._endpoints = {}

#    async def stop_endpoints(self):
#        for addr, ep in self._endpoints.items():
#            if not ep.closed():
#                await ep.write(msg=CTRL_SYNC, serializers=serde)
#                await ep.close()
#            del ep
#        self._endpoints = {}

    def stop_listener(self):
        if self._listener is not None:
            self._listener.stop()

    def __del__(self):
        self.abort_endpoints()
        self.stop_listener()

