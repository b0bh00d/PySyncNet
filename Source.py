#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Source                                                          #
#                                                                         #
# This is the functional module for the Source endpoint.                  #
#-------------------------------------------------------------------------#

import os
import lzma
import pickle
import asyncio
import collections

from typing         import Tuple

# https://aioquic.readthedocs.io/en/latest/design.html
from aioquic.asyncio import serve   # https://pypi.org/project/aioquic/
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.events import StreamDataReceived, HandshakeCompleted, ConnectionTerminated

import Types
import signaling    # https://pypi.org/project/signaling/

from Queue          import Queue
from Endpoint       import Endpoint
from Settings       import Settings
from FileSystem     import FileSystem
from Protocol       import Protocol
from Events         import Event
from Housekeeping   import Housekeeping

from Utilities      import format_number

from loguru         import logger   # https://pypi.org/project/loguru/

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Source(Endpoint):
    class ConnectionHandler(QuicConnectionProtocol):
        """ Quic connection handler """
        def __init__(self, parent, add_client_cb, del_client_cb, delta_cb, request_q: collections.deque, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self._connected = False

            self._parent: Source = parent
            self._client_connect_signal = signaling.Signal(args=['id', 'handler'])
            self._client_connect_signal.connect(add_client_cb)
            self._client_disconnect_signal = signaling.Signal(args=['id'])
            self._client_disconnect_signal.connect(del_client_cb)
            self._client_delta_signal = signaling.Signal(args=['id', 'client_fs'])
            self._client_delta_signal.connect(delta_cb)

            self._xfer_data = {}

            self._request_queue = request_q

            self._synchronized = False

        # https://github.com/aiortc/aioquic/issues/468
        # def __call__(self, *args, **kwargs):
        #     super().__init__(*args, **kwargs)
        #     return self

        def quic_event_received(self, event) -> None:
            if isinstance(event, HandshakeCompleted):
                self._client_connect_signal.emit(id=self._quic.host_cid, handler=self)

                logger.info(f"Client {self._quic.host_cid} connected")
                self._peer_id = self._quic.host_cid
                self._connected = True

            elif isinstance(event, StreamDataReceived):
                if event.stream_id not in self._xfer_data:
                    msg = Protocol.parse_packet(event.data)

                    match msg['cmd']:
                        case Protocol.SOURCE_EVENT:
                            assert False, "Source should not receive SOURCE_EVENT command"

                        case Protocol.SINK_FILESYSTEM_DELTA:
                            assert 'size' in msg, "Source message does not contain 'size'"
                            assert event.stream_id not in self._xfer_data, f"Duplicate stream identifier {event.stream_id} used for unique data transfer"

                            logger.info("Receiving file-system manifest from Sink...")
                            self._xfer_data[event.stream_id] = [msg['cmd'], msg['size'], b'']

                        case Protocol.SINK_REQUEST_FILE:
                            assert 'id' in msg, "Source message does not contain 'id'"
                            assert 'size' in msg, "Source message does not contain 'size'"
                            assert 'mtime' in msg, "Source message does not contain 'mtime'"
                            assert 'path' in msg, "Source message does not contain 'path'"

                            self._request_queue.append((self._quic.host_cid, msg))

                        case _:
                            assert False, f"Unknown command id {msg['cmd']} received"
                else:
                    self._xfer_data[event.stream_id][2] += event.data

                    if event.end_stream:
                        # the only binary data we will receive from a Sink is
                        # its file-system snapshot.  from it, we calculate a delta
                        # against our current Source file system, and then feed all
                        # calculated differences back to it as SOURCE_EVENTs

                        other_fs: FileSystem = pickle.loads(lzma.decompress(self._xfer_data[event.stream_id][2]))
                        logger.info("Calculating file-system delta...")
                        self._client_delta_signal.emit(id=self._quic.host_cid, client_fs=other_fs)

                        del self._xfer_data[event.stream_id]

            elif isinstance(event, ConnectionTerminated):
                if self._connected:
                    # client has disconnected
                    logger.info(f"Client {self._quic.host_cid} disconnected")
                    self._client_disconnect_signal.emit(id=self._quic.host_cid)

        async def send_command(self, data: bytes, end_stream=True) -> Tuple[bool, int]:
            if self._connected:
                stream_id = self._quic.get_next_available_stream_id()
                try:
                    self._quic.send_stream_data(stream_id, data, end_stream=end_stream)
                    self.transmit()
                except Exception:
                    return (False, 0)

                return (True, stream_id)
            else:
                return (False, 0)

        async def send_data(self, stream_id: int, local_stream: FileSystem.FileStream) -> bool:
            """
            Send binary data to a Sink

            Note: This data is never sent alone.  It always follows a command/event packet,
            using the same stream id
            """
            result = True
            buffer_size = int(Types.BUFFER_SIZE)

            try:
                data: bytes = await local_stream.read(buffer_size)
                while self._connected and (len(data) == buffer_size):
                    self._quic.send_stream_data(stream_id, data)
                    self.transmit()
                    data = await local_stream.read(buffer_size)
                # send the last of the stream data
                if self._connected:
                    self._quic.send_stream_data(stream_id, data, end_stream=True)
            except FileSystem.FileStream.Invalid:
                if self._connected:
                    self._quic.send_stream_data(stream_id, b'', end_stream=True)
            except FileSystem.FileStream.EndOfFile:
                if self._connected:
                    self._quic.send_stream_data(stream_id, data, end_stream=True)
            except asyncio.CancelledError:
                if self._connected:
                    self._quic.send_stream_data(stream_id, b'', end_stream=True)
                result = False

            if self._connected:
                self.transmit()

            return result

        async def ping(self):
            await super().ping()

        @property
        def id(self):
            return self._peer_id

        @property
        def synchronized(self):
            return self._synchronized

        @synchronized.setter
        def synchronized(self, value):
            if not isinstance(value, bool):
                raise ValueError("Value must be a Boolean type")
            self._synchronized = value

    def __init__(self, settings: Settings):
        super(Source, self).__init__(settings, type=Endpoint.TYPE_SOURCE)

        self._id = int(settings.source_id)

        # track the clients that are connected by caching their
        # handler instances
        self._clients = {}

        # this queue is for events on the Source file system.
        # as they occur, they are forwarded to each connected
        # Sink
        self._event_queue: Queue = None

        # this queue is for file-transfer requests from a Sink
        self._request_queue: collections.deque = collections.deque()

        self._watch_path = self._settings.source
        logger.info(f"Initializing FileSystem on path {self._watch_path}")

        self._file_system = FileSystem(settings, self._watch_path, self._id)

        self._xfer_tasks = {}

    async def _map_event_to_protcol(self, event: int) -> int:
        """ Map a file-system event type to a Protocol event type """
        match event:
            case Event.CREATED_FILE:
                return Protocol.EVENT_FILE_ADDED
            case Event.CREATED_FOLDER:
                return Protocol.EVENT_DIRECTORY_ADDED
            case Event.CREATED_SYMLINK:
                return Protocol.EVENT_SYMLINK_ADDED
            case Event.DELETED_FILE:
                return Protocol.EVENT_FILE_DELETED
            case Event.DELETED_FOLDER:
                return Protocol.EVENT_DIRECTORY_DELETED
            case Event.DELETED_SYMLINK:
                return Protocol.EVENT_SYMLINK_DELETED
            case Event.MODIFIED_FILE:
                return Protocol.EVENT_FILE_MODIFIED
            case Event.MODIFIED_FOLDER:
                assert False, "Modified folders are not supported"
            case Event.RENAMED_FILE:
                return Protocol.EVENT_FILE_RENAMED
            case Event.RENAMED_FOLDER:
                return Protocol.EVENT_DIRECTORY_RENAMED

    def _xfer_complete(self, task: asyncio.Task):
        """ Clean up things after a file transfer is complete """
        self._file_system.release_stream(self._xfer_tasks[task])
        del self._xfer_tasks[task]

    def _path_to_absolute(self, abs_path) -> str:
        """ Convert a path in relative form into one that points to a local watch instance """
        if abs_path is None:
            return abs_path
        assert abs_path.startswith('./'), f"Path '{abs_path}' not in required format"
        return self._watch_path + abs_path[1:]

    def _path_to_relative(self, full_path) -> str:
        """ Convert a path that references the local watch path to a relative value """
        if full_path is None:
            return full_path
        assert full_path.startswith(self._watch_path), f"Path '{full_path}' not in required format"
        return full_path.replace(self._watch_path, '.')

    def add_client_cb(self, **kwargs):
        """ Add a newly connected client to our tracking list """
        self._clients[kwargs['id']] = kwargs['handler']

    def del_client_cb(self, **kwargs):
        """ Remove a client from our tracking list """
        del self._clients[kwargs['id']]

    async def delta_cb(self, **kwargs):
        """
        Compare two file-system snapshots and feed the resulting deltas back
        to the client as SOURCE_EVENTs
        """
        async def send_event(client: Source.ConnectionHandler, event_data: Types.SourceEventTuple) -> bool:
            packet: bytes = Protocol.message_packet(Protocol.SOURCE_EVENT, event_data)
            result, _ = await client.send_command(packet)
            return result

        client = self._clients[kwargs['id']]
        other_fs = kwargs['client_fs']

        diff: Types.DiffTuple = await self._file_system.diff(other_fs)

        for old_name, new_name in diff.folders.moved:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_DIRECTORY_RENAMED,
                size=0,
                mtime=0,
                path1=old_name,
                path2=new_name,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for old_name, new_name in diff.files.moved:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_FILE_RENAMED,
                size=0,
                mtime=0,
                path1=old_name,
                path2=new_name,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for path, stat in diff.files.deleted:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_FILE_DELETED,
                size=stat.st_size,
                mtime=stat.st_mtime,
                path1=path,
                path2=None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for folder in diff.folders.deleted:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_DIRECTORY_DELETED,
                size=0,
                mtime=0,
                path1=folder,
                path2=None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for path, stat in diff.files.deleted:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_FILE_DELETED,
                size=stat.st_size,
                mtime=stat.st_mtime,
                path1=path,
                path2=None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for folder in diff.folders.added:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_DIRECTORY_ADDED,
                size=0,
                mtime=0,
                path1=folder,
                path2=None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for path, stat in diff.files.added:
            abs_path = self._path_to_absolute(path)
            is_symlink: bool = os.path.islink(abs_path)
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_SYMLINK_ADDED if is_symlink else Protocol.EVENT_FILE_ADDED,
                size=stat.st_size,
                mtime=stat.st_mtime,
                path1=path,
                path2=os.path.join('./', os.readlink(abs_path)) if is_symlink else None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        for path, stat in diff.files.modified:
            if not await send_event(client, Types.SourceEventTuple(
                event=Protocol.EVENT_FILE_MODIFIED,
                size=stat.st_size,
                mtime=stat.st_mtime,
                path1=path,
                path2=None,
                ids=None
                )):
                assert False, "Failed to send SOURCE_EVENT to client"

        client.synchronized = True

    # https://realpython.com/python-kwargs-and-args/
    def generate_handler(self, *args, **kwargs) -> QuicConnectionProtocol:
        """ Generate a connection handler instance for the QUIC protocol """
        return Source.ConnectionHandler(self,
                                        self.add_client_cb,
                                        self.del_client_cb,
                                        self.delta_cb,
                                        self._request_queue,
                                        *args, **kwargs)

    async def propagate_event(self, path: str) -> bool:
        """ Check the include/exclude filters to see if this event can be sent to clients """

        # account for the case where no filters are defined
        result = ('includes' not in self._settings) and ('excludes' not in self._settings)

        if 'includes' in self._settings:
            result = False
            for filter in self._settings.includes:
                if isinstance(filter, str):
                    if filter in path:
                        result = True
                        break
                else:   # regular expression
                    result = filter.search(path)
                    if result:
                        result = True
                        break
        elif 'excludes' in self._settings:
            result = True
            for filter in self._settings.excludes:
                if isinstance(filter, str):
                    if filter in path:
                        result = False
                        break
                else:   # regular expression
                    result = filter.search(path)
                    if result:
                        result = False
                        break

        return result

    async def event_loop(self, loop_pause_for: float = 0.3) -> None:
        """ Loop and process events for the Source """
        self._event_queue = self._file_system.monitor_events()
        logger.info(f"Monitoring {format_number(self._file_system.file_count)} files and {format_number(self._file_system.folder_count)} folders")

        housekeeping = Housekeeping()

        try:
            while self._loop:
                if not self._event_queue.is_empty():
                    event_data = self._event_queue.pop().data
                    p1=self._path_to_relative(event_data.path1)
                    p2=self._path_to_relative(event_data.path2)

                    logger.info(f"Received event {event_data.event.name}: {p1} {'' if p2 is None else '->'} {'' if p2 is None else p2}")

                    if await self.propagate_event(p1):
                        if self._clients:
                            # convert the event paths into relative format before
                            # sending to the Sinks
                            normalized_event = Types.SourceEventTuple(
                                event=await self._map_event_to_protcol(event_data.event),
                                size=event_data.size,
                                mtime=event_data.mtime,
                                path1=p1,
                                path2=p2,
                                ids=None
                            )
                            logger.info(f"Broadcasting {normalized_event.event.name}: {p1} {'' if p2 is None else '->'} {'' if p2 is None else p2}")
                            packet: bytes = Protocol.message_packet(Protocol.SOURCE_EVENT, normalized_event)
                            for id in self._clients:
                                # clients are notified of events on the Source file system,
                                # it is up to them to request content data that may have been
                                # added or modified (i.e., files)
                                #
                                # if a client has connected but has not yet synchronized its
                                # state with the Source, events are discarded until that is
                                # complete
                                if self._clients[id].synchronized:
                                    await self._clients[id].send_command(packet)
                        else:
                            logger.warning(f"No clients; ignoring {event_data.event.name}: {p1} {'' if p2 is None else '->'} {'' if p2 is None else p2}")
                    else:
                        logger.warning(f"Event filtered; discarding {event_data.event.name}: {p1} {'' if p2 is None else '->'} {'' if p2 is None else p2}")

                elif len(self._request_queue) != 0:
                    client_id, msg = self._request_queue.pop()
                    assert client_id in self._clients, f"Client id {client_id} not in client list"

                    client = self._clients[client_id]

                    local_path = self._path_to_absolute(msg['path'])
                    error: int = Protocol.ERROR_NO_ERROR
                    if os.path.exists(local_path):
                        stat = os.stat(local_path)
                        if (int(stat.st_mtime) == msg['mtime']) and (stat.st_size == msg['size']):
                            logger.info(f"Sending file contents for '{msg['path']}'")
                            packet: bytes = Protocol.message_packet(
                                Protocol.SOURCE_RESPONSE_FILE,
                                Types.ResponseTuple(id=msg['id'], path=msg['path'])
                            )
                            result, stream_id = await client.send_command(packet, end_stream=False)
                            if result:
                                stream = self._file_system.acquire_stream(local_path)
                                task = asyncio.create_task(client.send_data(stream_id, stream))
                                self._xfer_tasks[task] = stream
                                task.add_done_callback(self._xfer_complete)
                            else:
                                logger.error("Failed to send SOURCE_RESPONSE_FILE cmd to client")
                        else:
                            error = Protocol.ERROR_FILE_CHANGED
                    else:
                        error = Protocol.ERROR_FILE_DOES_NOT_EXIST

                    if error != Protocol.ERROR_NO_ERROR:
                        # tell them that their info is no longer valid
                        packet: bytes = Protocol.message_packet(
                            Protocol.ERROR_EVENT,
                            Types.ErrorEventTuple(event=error, path=msg['path'])
                        )
                        result, _ = client.send_command(packet)
                        assert result, f"Failed to send error event {error} to client"

                else:
                    housekeeping.perform()
                    await asyncio.sleep(loop_pause_for)

        except asyncio.CancelledError:
            self._loop = False
        except KeyboardInterrupt:
            self._loop = False

        # exit: clean up

        self._file_system.ignore_events()

        for id in self._clients:
            self._clients[id].close()
            await self._clients[id].wait_closed()

    async def execute(self) -> None:
        """ Entry point for the Source instance """
        logger.info("Source started.")

        configuration = QuicConfiguration(is_client=False,
                                          certificate=self._settings.certificate)
        configuration.load_cert_chain(self._settings.certificate, self._settings.keyfile)

        asyncio.create_task(serve(self._settings.address,
                                  self._settings.port,
                                  configuration=configuration,
                                  create_protocol=self.generate_handler))

        await self.event_loop()

        # stop any in-progress file transfers to all Sinks
        for task in self._xfer_tasks:
            task.cancel()

        # loop = asyncio.get_event_loop()
        # loop.create_task(self.watch_queue())
        # pending = [loop.create_task(self.watch_queue())
        #            #, loop.create_task(serve(self._settings.address, self._settings.port, configuration=configuration, create_protocol=self.generate_handler))
        #            ]
        # group = asyncio.gather(*pending, return_exceptions=True)
        # results = loop.run_until_complete(group)

        # print(f'Results: {results}')
