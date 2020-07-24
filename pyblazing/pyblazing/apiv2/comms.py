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


from dask.distributed import default_client

serde = ("cuda", "dask", "pickle", "error")

async def route_message(msg):
    print("calling route")
    worker = get_worker()
    if msg.metadata["add_to_specific_cache"] == "true":
        graph = worker.query_graphs[int(msg.metadata["query_id"])]
        #print(msg.metadata)
        print("Cacheid = " + msg.metadata["cache_id"])
        cache = graph.get_kernel_output_cache(
            int(msg.metadata["kernel_id"]),
            msg.metadata["cache_id"]
        )
        print("this is the route ")
        print(msg.data)
        cache.add_to_cache(msg.data)
    else:
        cache = worker.input_cache
        if(msg.data is None):
            import cudf
            msg.data = cudf.DataFrame()
        cache.add_to_cache_with_meta(msg.data, msg.metadata)
    print("done routing message")



<<<<<<< HEAD
    def __init__(self):
        self.dask_worker = get_worker()
        # create a single UCX instance for the lifetime of this worker
        if not hasattr(self.dask_worker, 'ucx'):
            # create a single UCX instance for the lifetime of this worker
            self.dask_worker.ucx = UCX()

        self.ucx = self.dask_worker.ucx
        self.stoprequest = asyncio.Event()
        self.request_cache = []

        import logging
        import sys
        root = logging.getLogger('ucx')
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)
#        logging.getLogger('ucx').addHandler(logging.StreamHandler(sys.stdout))

    async def get_ucx_address(self):
        print("Starting listeners",flush=True)
        self.ucx_address = await self.ucx.get_listener(route_message)

        return self.ucx_address

    async def create_endpoints(self, addr_map):
        await self.ucx.init_handlers(addr_map)

    async def start(self):
        async def work():
            while not self.stoprequest.is_set():
    #                print("Pull_from_cache",flush=True)
                await asyncio.sleep(0)
                have_data = self.dask_worker.output_cache.has_next_now()

                if have_data:
                    df, metadata = self.dask_worker.output_cache.pull_from_cache()
                    if metadata["add_to_specific_cache"] == "false":
                        #print("Should never get here!")
                        #print(metadata)
                        df = None
                    print(df,flush=True)
                    self.request_cache.append(await self.ucx.send(BlazingMessage(metadata, df)))

                    # clean up cache
                    self.request_cache = [f for f in self.request_cache if not f.done()]

            print('Finishing up')
            # finish up
            while self.dask_worker.output_cache.has_next_now():
                df, metadata = self.dask_worker.output_cache.pull_from_cache()
                if metadata["add_to_specific_cache"] == "false":
                    df = None
                self.request_cache.append(await self.ucx.send(BlazingMessage(metadata, df)))
            await asyncio.gather(*self.request_cache)
            self.request_cache = []
            print('Leaving worker')

        await work()

    async def stop(self, worker_task):
        self.stoprequest.set()
        await worker_task
        await self.ucx.flush_writes()
=======
class PollingPlugin:
    def __init__(self, *args, **kwargs):
        pass

    def setup(self, worker=None):
        self._worker = worker
        self._pc = PeriodicCallback(callback=self.async_run_polling, callback_time=100)
        self._pc.start()
        get_worker().polling = False


    async def async_run_polling(self):
        import asyncio, os

        worker = get_worker()
        if worker.polling == True:
            return
        worker.polling = True
        
        while self._worker.output_cache.has_next_now():            
            df, metadata = self._worker.output_cache.pull_from_cache()
            if metadata["add_to_specific_cache"] == "false" and len(df) == 0:
                df = None
            await UCX.get().send(BlazingMessage(metadata, df))
        worker.polling = False




CTRL_STOP = "stopit"

>>>>>>> aeb779f... removed all of the prints and couts that I wantonly littered throughout the codebase

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
        self.request_cache = []

    async def get_listener(self, callback):
#        if self._listener is not None:
#            if callback != self.callback:
#                raise RuntimeError("Updating the UCX listener callback is not implemented.")
#            return self._listener.address
        self.callback = callback
        return await self.start_listener()

    async def init_handlers(self, ucx_addresses):
        self.ucx_addresses = ucx_addresses
        print("addresses: "+ str(self.ucx_addresses),flush=True)
        eps = []
        for address in self.ucx_addresses.values():
            ep = await self.get_endpoint(address)
            print(ep,flush=True)

    @staticmethod
    def get_ucp_worker():
        return ucp.core._ctx.worker

    async def start_listener(self):

        async def handle_comm(comm):
            print("handling comm",flush=True)
            try:
                while not comm.closed():
                    print("%s- Listening!" % get_worker().address,flush=True)
                    msg = await asyncio.shield(comm.read())
                    print("%s- got msg: %s" % (get_worker().address, msg),flush=True)

                    msg = BlazingMessage(**{k: v.deserialize()
                                            for k, v in msg.items()})
                    self.received += 1
                    print("%d messages received on %s" % (self.received, get_worker().address),flush=True)
                    if "message_id" in msg.metadata:

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
        self.request_cache = []

    async def get_listener(self, callback):
#        if self._listener is not None:
#            if callback != self.callback:
#                raise RuntimeError("Updating the UCX listener callback is not implemented.")
#            return self._listener.address
        self.callback = callback
        return await self.start_listener()

    async def init_handlers(self, ucx_addresses):
        self.ucx_addresses = ucx_addresses
        print("addresses: "+ str(self.ucx_addresses),flush=True)
        eps = []
        for address in self.ucx_addresses.values():
            ep = await self.get_endpoint(address)
            print(ep,flush=True)

    @staticmethod
    def get_ucp_worker():
        return ucp.core._ctx.worker

    async def start_listener(self):

        async def handle_comm(comm):
            print("handling comm",flush=True)
            try:
                while not comm.closed():
                    print("%s- Listening!" % get_worker().address,flush=True)
                    msg = await asyncio.shield(comm.read())
                    print("%s- got msg: %s" % (get_worker().address, msg),flush=True)

                    msg = BlazingMessage(**{k: v.deserialize()
                    msg = BlazingMessage(**{k: v.deserialize()
                                            for k, v in msg.items()})
                    self.received += 1
                    print("%d messages received on %s" % (self.received, get_worker().address),flush=True)
                    if "message_id" in msg.metadata:
                        print("Finished receiving message id: "+ str(msg.metadata["message_id"]),flush=True)
                    else:
                        print("No message_id",flush=True)
                    print("Invoking callback",flush=True)
                    await self.callback(msg)
                    print("Done invoting callback",flush=True)
            except CancelledError:
                pass
            except Exception as e:
                print('Error in callback: {}'.format(e),flush=True)
                traceback.print_tb(e.__traceback__)
                print('traceback',flush=True)
                raise

            print("Listener shutting down",flush=True)

        try:
            ip, port = parse_host_port(get_worker().address)

            print("Constructing listener on loop ",asyncio.get_running_loop(),flush=True)
            print("with policy ",asyncio.get_event_loop_policy(),flush=True)
            self._listener = await UCXListener(ip, handle_comm)

            print("Starting listener on worker",flush=True)
            await self._listener.start()

            print("Started listener on port " + str(self.listener_port()),flush=True)


        return "ucx://%s:%s" % (ip, self.listener_port())
        except Exception as e:

    def listener_port(self):
        return self._listener.port

    async def _create_endpoint(self, addr):
        ep = await UCXConnector().connect(addr)
        self._endpoints[addr] = ep
        print("Created endpoint: " + str(ep),flush=True)
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
        print("calling send: "+ str(blazing_msg.metadata),flush=True)

        local_dask_addr = self.ucx_addresses[get_worker().address]
        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
        local_dask_addr = get_worker().ucx_addresses[get_worker().address]
        if blazing_msg.metadata["sender_worker_id"] in blazing_msg.metadata["worker_ids"]:

        for dask_addr in blazing_msg.metadata["worker_ids"]:
            # Map Dask address to internal ucx endpoint address
            addr = get_worker().ucx_addresses[dask_addr]
            try:
            ep = await self.get_endpoint(addr)
            try:
                to_ser = {"metadata": to_serialize(blazing_msg.metadata)}
            
                if blazing_msg.data is not None:
                if blazing_msg.data is not None:
                    to_ser["data"] = to_serialize(blazing_msg.data)
                    print(str(blazing_msg.data))
            except:
                print("An error occurred in serialization",flush=True)

            try:
                print('Before write',flush=True)
                # dont' await the call to write, to avoid deadlock
                # await ep.write(msg=to_ser, serializers=serde)
                # https://github.com/rapidsai/ucx-py/issues/140
                task = asyncio.create_task(ep.write(msg=to_ser, serializers=serde))
                await task # just testing
                print('After write',flush=True)
            except:
                print("Error occurred during write",flush=True)
            self.sent += 1
            print("%d messages sent on %s" % (self.sent, get_worker().address),flush=True)
            print("seems like it wrote",flush=True)
            except:
                print("Error occurred during write",flush=True)
            self.sent += 1
            print("%d messages sent on %s" % (self.sent, get_worker().address),flush=True)
            print("seems like it wrote",flush=True)
            return task

    async def flush_writes(self):
        print('Progress...',flush=True)
        await ucp.flush()
        print('done.',flush=True)

    def abort_endpoints(self):
        for addr, ep in self._endpoints.items():
            if not ep.closed():

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

    async def barrier(self):
        for addr, ep in self._endpoints.items():
            try:
                await ep.write(msg=CTRL_SYNC, serializers=serde)
            except dask.distributed.CommClosedError:
                print('Endpoint {} has closed prematurely.\n'.format(ep),flush=True)

        print('Barrier: sent all syncs',flush=True)
        for addr, ep in self._endpoints.items():
            try:
                await ep.write(msg=CTRL_SYNC, serializers=serde)
            except dask.distributed.CommClosedError:
                print('Endpoint {} has closed prematurely.\n'.format(ep),flush=True)

        print('Barrier: sent all syncs',flush=True)
        while True:
            await asyncio.sleep(0)
            async with self.lock:
                if self.sync_recvd == len(self._endpoints):
                    self.sync_recvd = 0
                    return

    def stop_listener(self):

    def stop_listener(self):
        if self._listener is not None:
            self._listener.stop()

    def __del__(self):
        print("Cleaning up",flush=True)
        self.abort_endpoints()
        self.stop_listener()

