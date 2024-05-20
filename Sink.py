#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Sink                                                            #
#                                                                         #
# This is the functional module for the Sink endpoint.  One or more Sink  #
# endpoints are allowed.                                                  #
#-------------------------------------------------------------------------#

import os
import ssl
import lzma
import random
import shutil
import pickle
import asyncio
import collections

from typing     import Callable, Tuple, Dict, AnyStr, Any, cast

from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted, StreamDataReceived, ConnectionTerminated

import Types

from Endpoint   import Endpoint
from FileSystem import FileSystem
from Protocol   import Protocol
from Utilities  import calculate_fingerprint
from MD5        import MD5

from loguru     import logger

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Sink(Endpoint):
    class RecieveHandler:
        """ Handler to manage file data received from the Source """
        class FileCreateError(Exception):
            pass

        class DiskSpaceError(Exception):
            pass

        def __init__(self, xfer_id: int, byte_count: int, mtime: int, path: str = None, completion: Callable = None, offset: int = None):
            self._error = Protocol.ERROR_NO_ERROR

            self._id = xfer_id
            self._mtime = mtime
            self._receiving = byte_count
            self._received = 0
            self._completion = completion

            self._path = path
            self._output_data = None
            self._path_data = None
            self._path_meta = None
            self._md5 = None

            if path is not None:
                self._path_data = path + FileSystem.PART_EXT
                self._path_meta = path + FileSystem.META_EXT

                if os.path.exists(self._path_data):
                    if os.path.exists(self._path_meta):
                        try:
                            with open(self._path_meta, 'rb') as f:
                                state = f.read()
                                self._md5 = MD5()
                                if not self._md5.setState(state):
                                    logger.warning(f"Failed to refresh MD5 state from '{self._path_meta}'")
                                    self._md5 = None
                        except Exception:
                            pass

                    if self._md5 is None:
                        # refresh the MD5 value directly from the partial file;
                        # obviously slow, but might not be nearly as slow as starting
                        # the transfer all over again
                        logger.info(f"Reconstructing MD5 state from '{self._path_meta}'")
                        self._md5 = calculate_fingerprint(self._path_data)

                    try:
                        self._output_data = open(self._path_data, 'ab')
                    except Exception:
                        raise Sink.RecieveHandler.FileCreateError(f"Could not append to partial file '{self._path_data}'")
                else:
                    try:
                        self._output_data = open(self._path_data, 'wb')
                    except Exception:
                        raise Sink.RecieveHandler.FileCreateError(f"Could not create new file '{self._path_data}'")

                try:
                    self._output_meta = open(self._path_meta, 'wb')
                except Exception:
                    raise Sink.RecieveHandler.FileCreateError(f"Could not create new file '{self._path_meta}'")

                if self._md5 is None:
                    self._md5 = MD5()

                if self._error == Protocol.ERROR_NO_ERROR:
                    # size it to make sure there's enough room available
                    # on the file system before we start requesting data
                    try:
                        self._output_data.seek(self._receiving - 1)
                        self._output_data.write(b'\0')
                        self._output_data.seek(0)
                    except Exception:
                        raise Sink.RecieveHandler.DiskSpaceError(f"Could not allocate {self._receiving} bytes to receive file contents")

            self._data = None

        def __del__(self):
            # if we completed and cleaned up gracefully, this will not exist
            if os.path.exists(self._path_data):
                if self._output_data:
                    self._output_data.close()
                    self._output_data = None
                if self._output_meta:
                    self._output_meta.close()
                    self._output_meta = None

                # we leave the partial download in place, along with its metadata,
                # so it can be resumed the next time we synchronized the file
                # systems with the server

            self._data = None

        def error(self): return self._error

        def add(self, data: bytes, eos: bool = False) -> bool:
            self._received += len(data)

            # update the running MD5 value for what we've received so far
            self._md5.update(data)

            # cache it to disk
            self._output_meta.write(self._md5.getState())
            # rewinding the stream back to zero switches it to read mode
            self._output_meta.seek(0)
            # a second seek is required to switch back to write mode
            self._output_meta.seek(0)

            if self._output_data:
                self._output_data.write(data)
            else:
                if not self._data:
                    self._data = []
                self._data += data

            if eos and (self._received == self._receiving):
                # we've gotten all the bytes we're expecting
                if self._output_data:
                    self._output_data.close()    # make sure we flush output buffers
                    self._output_data = None
                    self._output_meta.close()    # make sure we flush output buffers
                    self._output_neta = None
                    os.utime(self._path_data, (self._mtime, self._mtime))

                if self._completion:
                    # invoke our completion callback
                    self._completion(self._id)

                return True

            return False

    class DataHandler(QuicConnectionProtocol):
        """ QUIC connection handler """
        def __init__(self, parent, event_q: collections.deque, data_q: collections.deque, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._connected = False

            self._parent: Sink = parent

            # holds incoming events from the Source
            self._event_queue = event_q

            # holds incoming data from the Source, each entry
            # is a tuple(transfer_id, bytes)
            self._data_queue = data_q

            self._xfer_data = {}

        # https://github.com/aiortc/aioquic/issues/468
        # def __call__(self, *args, **kwargs):
        #     super().__init__(*args, **kwargs)
        #     return self

        def quic_event_received(self, event):
            if isinstance(event, HandshakeCompleted):
                self._connected = True
                logger.info(f"Client connected to {self._quic.host_cid}")

            elif isinstance(event, StreamDataReceived):
                if event.stream_id not in self._xfer_data:
                    msg = Protocol.parse_packet(event.data)

                    match msg['cmd']:
                        case Protocol.SOURCE_EVENT:
                            # command packet coming FROM the Source
                            self._event_queue.append(msg)

                        case Protocol.SOURCE_RESPONSE_FILE:
                            assert 'id' in msg, "Source message does not contain 'id'"
                            assert 'path' in msg, "Source message does not contain 'path'"
                            assert event.stream_id not in self._xfer_data, f"Duplicate stream identifier {event.stream_id} used for unique data transfer"

                            self._xfer_data[event.stream_id] = [msg['id'], msg['path']]

                        case Protocol.SINK_FILESYSTEM_DELTA:
                            assert False, "Sink should not receive SINK_FILESYSTEM_DELTA command"
                        case Protocol.SINK_REQUEST_FILE:
                            assert False, "Sink should not receive SINK_REQUEST_FILE command"

                        case _:
                            assert False, f"Unknown cmd encountered {msg['cmd']}"
                else:
                    id = self._xfer_data[event.stream_id][0]

                    if (len(event.data) == 0) and event.end_stream:
                        # this is a cancelation indicator
                        logger.error(f"Transfer of {self._xfer_data[event.stream_id][1]} was cancelled by Source")
                        del self._xfer_data[event.stream_id]
                        self._parent.xfer_complete(id)
                    else:
                        handler = self._parent.get_xfer_handler(id)
                        if handler.add(event.data, event.end_stream):
                            # we're done with this transfer and the handler
                            # has invoked the completion callback
                            del self._xfer_data[event.stream_id]
                            self._parent.xfer_complete(id)

            elif isinstance(event, ConnectionTerminated):
                if self._connected:
                    # Source has disconnected
                    logger.warning("Source disconnected")
                    self._connected = False
                    # self._client_disconnect_signal(self.host_cid)
                else:
                    # if we never connected, there's nothing to clean up
                    pass

        async def send_command(self, data: bytes, end_stream=True) -> Tuple[bool, int]:
            """ Send a small command/event packet to the Sink """
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

            try:
                data: bytes = await local_stream.read(Types.BUFFER_SIZE)
                while self._connected and (len(data) == Types.BUFFER_SIZE):
                    self._quic.send_stream_data(stream_id, data)
                    data = await local_stream.read(Types.BUFFER_SIZE)
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
        def is_connected(self) -> bool:
            return self._connected

    def __init__(self, settings):
        super(Sink, self).__init__(settings, type=Endpoint.TYPE_SINK)

        self._settings = settings

        self._id = int(settings.sink_id)

        self._connection = None
        self._handler = None
        self._quic_config: QuicConfiguration = None

        self._watch_path = self._settings.sink
        logger.info(f"Initializing FileSystem on path {self._watch_path}")

        self._file_system = FileSystem(self._settings, self._watch_path, self._id)

        self._event_queue: collections.deque = collections.deque()
        self._data_queue: collections.deque = collections.deque()

        self._xfer_handlers = {}

        self._springboard = {
            Protocol.EVENT_DIRECTORY_DELETED : self.del_dir,
            Protocol.EVENT_DIRECTORY_RENAMED : self.ren_dir,
            Protocol.EVENT_DIRECTORY_ADDED : self.add_dir,
            Protocol.EVENT_FILE_DELETED : self.del_file,
            Protocol.EVENT_FILE_RENAMED : self.ren_file,
            Protocol.EVENT_FILE_ADDED : self.add_file,
            Protocol.EVENT_FILE_MODIFIED : self.mod_file,
            Protocol.EVENT_FILE_PARTIAL : self.resume_file,
            Protocol.EVENT_SYMLINK_ADDED : self.add_symlink,
            Protocol.EVENT_SYMLINK_DELETED : self.del_symlink,
        }

    def _path_to_absolute(self, abs_path) -> str:
        """ Convert a path in relative form into one that points to a local watch instance """
        if not os.path.exists(abs_path):
            assert abs_path.startswith('./'), f"Path '{abs_path}' not in required format"
            return self._watch_path + abs_path[1:]
        return abs_path

    def _path_to_relative(self, full_path) -> str:
        """ Convert a path that references the local watch path to a relative value """
        if os.path.exists(full_path):
            assert full_path.startswith(self._watch_path), f"Path '{full_path}' not in required format"
            return full_path.replace(self._watch_path, '.')
        return full_path

    def get_xfer_handler(self, id: int) -> RecieveHandler:
        assert id in self._xfer_handlers, f"Provided id {id} does not exist in _xfer_handlers"
        return self._xfer_handlers[id]

    def xfer_complete(self, id: int) -> None:
        assert id in self._xfer_handlers, f"Provided id {id} does not exist in _xfer_handlers"
        del self._xfer_handlers[id]

    async def del_dir(self, data: Dict[AnyStr, Any]) -> None:
        """ Remove a directory, and all of its contents, from the local watch path """
        dir_path = self._path_to_absolute(data['path1'])

        assert os.path.exists(dir_path), f"Event path '{dir_path}' does not exist"
        assert os.path.isdir(dir_path), f"Event path '{dir_path}' is not a directory"

        shutil.rmtree(dir_path,
                      onerror=lambda function, path, excinfo: logger.error(f"Failed to remove directory {data.path1}"))
        logger.info(f"Deleted folder '{data['path1']}'")

    async def ren_dir(self, data: Dict[AnyStr, Any]) -> None:
        """ Rename a directory on the local watch path """
        orig_path = self._path_to_absolute(data['path1'])
        new_path = self._path_to_absolute(data['path2'])

        assert os.path.exists(orig_path), f"Event path '{orig_path}' does not exist"
        assert not os.path.exists(new_path), f"Event path '{new_path}' already exists"
        assert os.path.isdir(orig_path), f"Event path '{orig_path}' is not a directory"

        try:
            os.rename(orig_path, new_path)
            logger.info(f"Renamed folder '{data['path1']}' -> '{data['path2']}'")
        except Exception:
            logger.error(f"Failed to rename directory '{data['path1']}' to '{data['path2']}'")

    async def add_dir(self, data: Dict[AnyStr, Any]) -> None:
        """ Create a new directory on the local watch path """
        new_path = self._path_to_absolute(data['path1'])

        assert not os.path.exists(new_path), f"Event path '{new_path}' already exists"

        try:
            os.mkdir(new_path)
            logger.info(f"Added new folder '{data['path1']}'")
        except Exception:
            logger.error(f"Failed to add directory '{data['path1']}'")

    async def del_file(self, data: Dict[AnyStr, Any]) -> None:
        """ Remove a file from the local watch path """
        file_path = self._path_to_absolute(data['path1'])

        assert os.path.exists(file_path), f"Event path '{file_path}' does not exist"
        if os.path.islink(file_path):
            await self.del_symlink(data)
        else:
            assert os.path.isfile(file_path), f"Event path '{file_path}' is not a file"

            try:
                os.remove(file_path)
                logger.info(f"Deleted existing file '{data['path1']}'")
            except Exception:
                logger.error(f"Failed to delete file '{data['path1']}'")

    async def ren_file(self, data: Dict[AnyStr, Any]) -> None:
        """ Rename a file on the local watch path """
        orig_path = self._path_to_absolute(data['path1'])
        new_path = self._path_to_absolute(data['path2'])

        assert os.path.exists(orig_path), f"Event path '{orig_path}' does not exist"
        assert not os.path.exists(new_path), f"Event path '{new_path}' already exists"
        assert os.path.isfile(orig_path), f"Event path '{orig_path}' is not a file"

        try:
            os.rename(orig_path, new_path)
            logger.info(f"Renamed existing file '{data['path1']}' to '{data['path2']}'")
        except Exception:
            logger.error(f"Failed to rename file '{data['path1']}' to '{data['path2']}'")

    async def add_file(self, data: Dict[AnyStr, Any]) -> None:
        """ Add a file on the local watch path, receiving its contents from the Source """
        new_path = self._path_to_absolute(data['path1'])

        assert not os.path.exists(new_path), f"Event path '{new_path}' already exists"

        if data['size'] == 0:
            # this is just a 'touch'; no contents to transfer
            open(new_path, 'wb')
            os.utime(new_path, (data['mtime'], data['mtime']))
            logger.info(f"Touched new file '{data['path1']}'")
        else:
            # set up to recieve the file contents
            handler: Sink.RecieveHandler = None
            id = random.getrandbits(32)
            try:
                handler = Sink.RecieveHandler(id, byte_count=data['size'], mtime=data['mtime'], path=new_path, completion=self.add_file_complete)
            except Sink.RecieveHandler.FileCreateError as error:
                logger.error(error.message)
            except Sink.RecieveHandler.DiskSpaceError as error:
                logger.error(error.message)
            finally:
                if handler:
                    # we're ready to rock 'n roll...
                    self._xfer_handlers[id] = handler

                    # request the file contents from the Source
                    cmd = Protocol.message_packet(Protocol.SINK_REQUEST_FILE,
                                                Types.RequestTuple(id=id, size=data['size'], mtime=data['mtime'], path=data['path1']))

                    logger.info(f"Requesting file '{data['path1']}' from Source")
                    result = await self._handler.send_command(cmd)
                    assert result, "DataHandler.send_command() failed"
                else:
                    raise

    def add_file_complete(self, id: int) -> None:
        """ Process results after file contents have been transferred from the Source """
        assert id in self._xfer_handlers, f"Invalid id {id} provided"

        handler = self._xfer_handlers[id]

        assert os.path.exists(handler._path_data)

        try:
            os.rename(handler._path_data, handler._path)
            if os.path.eixsts(handler._path_meta):
                os.remove(handler._path_meta)
            logger.info(f"Added new file '{self._path_to_relative(handler._path)}'")
        except Exception:
            logger.error(f"Failed to rename temp file '{handler._path_data}' -> '{handler._path}'")

    async def mod_file(self, data: Dict[AnyStr, Any]) -> None:
        """ Process a modified file on the local watch path, receiving its contents from the Source """
        full_path = self._path_to_absolute(data['path1'])

        if not os.path.exists(full_path):
            # this is likely the result of the system
            # issuing an ADD_FILE followed immediately by
            # MOD_FILE.  punt this to add_file()
            await self.add_file(data)
        elif data['size'] == 0:
            # truncate it; there's no contents to transfer
            open(full_path, 'wb')
            os.utime(full_path, (data['mtime'], data['mtime']))
            logger.info(f"Truncated existing file '{self._path_to_relative(data['path1'])}'")
        else:
            # set up to recieve the file contents
            id = random.getrandbits(32)
            try:
                handler = Sink.RecieveHandler(id, byte_count=data['size'], mtime=data['mtime'], path=full_path, completion=self.mod_file_complete)
            except Sink.RecieveHandler.FileCreateError as error:
                logger.error(error.message)
            except Sink.RecieveHandler.DiskSpaceError as error:
                logger.error(error.message)
            finally:
                # we're ready to rock 'n roll...
                self._xfer_handlers[id] = handler

                # request the file contents from the Source
                cmd = Protocol.message_packet(Protocol.SINK_REQUEST_FILE,
                                            Types.RequestTuple(id=id, size=data['size'], mtime=data['mtime'], path=data['path1']))

                logger.info(f"Requesting file '{data['path1']}' from Source")
                result = await self._handler.send_command(cmd)
                assert result, "DataHandler.send_command() failed"

    def mod_file_complete(self, id: int) -> None:
        """ Process results after file contents have been transferred from the Source """
        assert id in self._xfer_handlers, f"Invalid id {id} provided"

        handler = self._xfer_handlers[id]

        assert os.path.exists(handler._path)
        assert os.path.exists(handler._path_data)

        backup_name = handler._path + '.' + str(random.getrandbits(32))

        # 1. rename the original file
        try:
            os.rename(handler._path, backup_name)
        except Exception:
            logger.error(f"Failed to back up original file '{handler._path}' -> '{backup_name}'")

        if os.path.exists(backup_name):
            # 2. rename the temp file
            try:
                os.rename(handler._path_data, handler._path)
                if os.path.eixsts(handler._path_meta):
                    os.remove(handler._path_meta)
            except Exception:
                logger.error(f"Failed to rename cache file '{handler._path_data}' -> '{handler._path}'")
                os.rename(backup_name, handler._path)

            if os.path.exists(handler._path):
                # 3. remove the backup file
                try:
                    os.remove(backup_name)
                    logger.info(f"Updated existing file '{self._path_to_relative(handler._path)}'")
                except Exception:
                    logger.error(f"Failed to remove backup file '{backup_name}'")
                    os.rename(handler._path, handler._path_data)
                    os.rename(backup_name, handler._path)

    async def resume_file(self, data: Dict[AnyStr, Any]) -> None:
        """ Add a file on the local watch path, receiving its contents from the Source """
        file_path = self._path_to_absolute(data['path1'])

        if os.path.exists(file_path):
            # this was originally a mod_file event
            pass
        else:
            # this was originally an add_file event
            pass

        assert ('offset' in data) and (data['offset'] != 0), "Missing or invalid 'offset' value"

        # set up to recieve the file contents
        handler: Sink.RecieveHandler = None
        id = random.getrandbits(32)
        try:
            handler = Sink.RecieveHandler(id, byte_count=data['size'], mtime=data['mtime'], path=file_path, completion=self.add_file_complete)
        except Sink.RecieveHandler.FileCreateError as error:
            logger.error(error.message)
        except Sink.RecieveHandler.DiskSpaceError as error:
            logger.error(error.message)
        finally:
            if handler:
                # we're ready to rock 'n roll...
                self._xfer_handlers[id] = handler

                # request the file contents from the Source
                cmd = Protocol.message_packet(Protocol.SINK_RESUME_FILE,
                                            Types.RequestTuple(id=id, size=data['size'], mtime=data['mtime'], path=data['path1']))

                logger.info(f"Requesting file '{data['path1']}' from Source")
                result = await self._handler.send_command(cmd)
                assert result, "DataHandler.send_command() failed"
            else:
                raise

    async def add_symlink(self, data: Dict[AnyStr, Any]) -> None:
        """ Add a new symlink on the local watch path """
        sym_source = data['path1']#self._path_to_absolute(data['path1'])
        sym_target = data['path2'].replace('./', '')#self._path_to_absolute(data['path2'])

        assert not os.path.exists(sym_source), f"Event path '{sym_source}' already exists"

        cwd = os.getcwd()
        os.chdir(self._watch_path)

        try:
            os.symlink(sym_target, sym_source)
            logger.info(f"Added new symlink '{data['path1']}' -> '{data['path2']}'")
        except Exception:
            logger.error(f"Failed to create symlink '{data['path1']}' -> '{data['path2']}'")

        os.chdir(cwd)

    async def del_symlink(self, data: Dict[AnyStr, Any]) -> None:
        """ Remove an existing symlink from the local watch path """
        sym_path = self._path_to_absolute(data['path1'])

        assert os.path.exists(sym_path), f"Event path '{sym_path}' does not exist"
        assert os.path.islink(sym_path), f"Event path '{sym_path}' is not a symlink"

        try:
            os.unlink(sym_path)
            logger.info(f"Deleted existing symlink '{data['path1']}'")
        except Exception:
            logger.error(f"Failed to delete symlink '{data['path1']}'")

    def generate_handler(self, *args, **kwargs) -> QuicConnectionProtocol:
        handler = Sink.DataHandler(self, self._event_queue, self._data_queue, *args, **kwargs)
        return handler

    async def event_loop(self, loop_pause_for: float = 0.3) -> None:
        """ Loop and process events for the Source """
        # handler = Sink.DataHandler(self, self._event_queue, self._data_queue)

        ping_amount = Protocol.PING_INTERVAL // loop_pause_for

        do_run: bool = True
        while do_run:
            ping_countdown = ping_amount
            logger.info(f"Attempting to connect to Source @ '{self._settings.address}:{self._settings.port}'...")
            try:
                async with connect(self._settings.address,
                                   int(self._settings.port),
                                   configuration=self._quic_config,
                                   create_protocol=self.generate_handler) as client:

                    logger.info(f"Connected to Source @ '{self._settings.address}:{self._settings.port}'...")

                    self._handler = cast(Sink.DataHandler, client)

                    # send our FileSystem image so the Source can generate
                    # a diff and enqueue those differences back to us

                    logger.info("Pickling and compressing file system image")
                    pickled_data = lzma.compress(pickle.dumps(self._file_system))
                    # logger.info(f"Pickled file system is {len(pickled_data)} bytes")
                    # xfer_id = random.getrandbits(32)
                    packet: bytes = Protocol.message_packet(Protocol.SINK_FILESYSTEM_DELTA, Types.SyncTuple(size=len(pickled_data)))

                    # give the Source a heads-up that the data is coming
                    result, stream_id = await self._handler.send_command(packet, end_stream=False)
                    assert result, "DataHandler.send_command() failed"

                    logger.info("Transmitting file-system manifest to Source...")
                    # now send it on the data channel
                    result = await self._handler.send_data(stream_id, self._file_system.acquire_stream(pickled_data))
                    assert result, "DataHandler.send_data() failed"

                    logger.info("Sink now responding to Source events.")

                    try:
                        # dispatch events and data from the Source
                        while self._handler.is_connected:
                            # see if any events have occurred on the Source
                            try:
                                item = self._event_queue.pop()

                                assert item['event'] in self._springboard, f"Unkown event type encountered from Source: {item['event']}"
                                await self._springboard[item['event']](item)
                            except IndexError:
                                pass

                            # see if we've recevied data on any active transfers
                            try:
                                id, data = self._data_queue.pop()

                                assert id in self._xfer_handlers, f"Transfer id {id} does not exist"

                                if self._xfer_handlers[id].add(data):
                                    # this transfer is complete
                                    del self._xfer_handlers[id]
                            except IndexError:
                                pass

                            # if we've nothing to process, give away our time slice
                            if (len(self._event_queue) == 0) and (len(self._data_queue) == 0):

                                ping_countdown -= 1
                                if ping_countdown == 0:
                                    if self._handler.is_connected:
                                        await self._handler.ping()
                                    ping_countdown = ping_amount

                                await asyncio.sleep(loop_pause_for)

                    except ConnectionError:
                        self._file_system = None

                    except asyncio.CancelledError:
                        do_run = False
                    except KeyboardInterrupt:
                        do_run = False

                    if self._file_system is None:
                        # we lost the connection; make sure our file system image
                        # is current before we try to reconnect

                        self._file_system = FileSystem(self._settings, self._watch_path, self._id)

            except ConnectionError:
                logger.info(f"Failed to connect to Source @ '{self._settings.address}:{self._settings.port}'")
                await asyncio.sleep(self._settings.retry)

            except asyncio.CancelledError:
                do_run = False
            except KeyboardInterrupt:
                do_run = False

    async def execute(self) -> None:
        """ Entry point for the Sink instance """
        self._quic_config = QuicConfiguration(is_client=True, verify_mode=ssl.CERT_NONE)

        try:
            await self.event_loop()
        except asyncio.CancelledError:
            logger.info("got asyncio.CancelledError")
        except KeyboardInterrupt:
            logger.info("got KeyboardInterrupt")

        # exit: clean up
