#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Protocol                                                        #
#                                                                         #
# Exposes enums for event communications in the multicast group           #
#-------------------------------------------------------------------------#

from enum       import IntEnum
from typing     import Dict, Any

import Types

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Protocol(IntEnum):
    """ Protocol enums """

    # QUIC channel definitions

    CHANNEL_COMMAND = 0
    CHANNEL_DATA = 1

    PING_INTERVAL = 30              # how often (in seconds) should we ping a Source to keep the connection alive?

    # administrative commands

    SOURCE_EVENT = 100              # an event has occurred on the Source file system (EventStruct)
    SOURCE_SYNC_CANCEL = 101        # cancel an in-progress file transfer
    SOURCE_SYNC_INVALID = 102       # a requested file transfer is no longer valid (size or mtime no longer match)
    SOURCE_RESPONSE_FILE = 103      # a requested file transfer is starting

    SINK_FILESYSTEM_DELTA = 200     # a freshly connected client is sending their file system image
    SINK_REQUEST_FILE = 201         # Sink is requesting a file transfer (RequestStruct)
    SINK_RESUME_FILE = 202          # Sink is requesting a file transfer from a given offset (ResumeStruct)

    # file-system events (generated only by the Source endpoint)

    EVENT_DIRECTORY_DELETED = 300   # a directory on the file system has been deleted
    EVENT_DIRECTORY_ADDED = 301     # a direcotry has been added to the file system
    EVENT_DIRECTORY_RENAMED = 302   # a direcotry has been renamed (which might also move it)
    EVENT_FILE_DELETED = 303        # a file has been deleted
    EVENT_FILE_RENAMED = 304        # a file has been renamed (which might also move it)
    EVENT_FILE_ADDED = 305          # a new file has appeared on the file system
    EVENT_FILE_MODIFIED = 306       # an existing file has changed
    EVENT_FILE_PARTIAL = 307        # a partial file transfer has been validated as resumeable by the Source
    EVENT_SYMLINK_ADDED = 308       # a symlink entry has been created
    EVENT_SYMLINK_DELETED = 309     # a symlink has been deleted (pretty much the same as FILE_DELETED)

    # error messages

    ERROR_EVENT = 500               # error event type
    ERROR_NO_ERROR = 501            # no error (initialization value)
    ERROR_SPACE = 502               # there is not enough free space on the Sink file system to capture the incoming file data
    ERROR_CREATE_FILE = 503         # for some reason, a file could not be created on the target file system
    ERROR_FILE_CHANGED = 504        # a file requested by a Sink has changed since the event was received
    ERROR_FILE_DOES_NOT_EXIST = 505 # a file requested by a Sink no longer exists

    @staticmethod
    def message_packet(cmd, data=None) -> bytes:
        """ Contruct a message packet """

        msg = Types.CommandStruct.build(dict(cmd=cmd))

        # the message payload will be different for each command

        match cmd:
            case Protocol.SOURCE_EVENT:
                assert data is not None, "SOURCE_EVENT requires event data"
                assert isinstance(data, Types.SourceEventTuple), "SOURCE_EVENT requires SourceEventTuple data"

                return msg + Types.EventStruct.build(dict(
                    event=data.event,
                    size=data.size, #0 if stat_value is None else stat_value.st_size,
                    mtime=int(data.mtime), #0 if stat_value is None else stat_value.st_mtime,
                    path1=data.path1,
                    path2=data.path2 if data.path2 else ''))

            case Protocol.SOURCE_RESPONSE_FILE:
                assert data is not None, "SOURCE_RESPONSE_FILE requires file data"
                assert isinstance(data, Types.ResponseTuple), "SOURCE_RESPONSE_FILE requires ResponseTuple data"

                return msg + Types.ResponseStruct.build(dict(
                    id=data.id,
                    path=data.path
                ))

            case Protocol.SINK_FILESYSTEM_DELTA:
                assert data is not None, "SINK_FILESYSTEM_DELTA requires file data"
                assert isinstance(data, Types.SyncTuple), "SINK_FILESYSTEM_DELTA requires SyncTuple data"

                return msg + Types.SyncStruct.build(dict(
                    # id=data.id,
                    size=data.size
                ))

            case Protocol.SINK_REQUEST_FILE:
                # we won't get a command-channel response to this
                assert data is not None, "SINK_REQUEST_FILE requires file information"
                assert isinstance(data, Types.RequestTuple), "SINK_REQUEST_FILE requires RequestTuple data"

                return msg + Types.RequestStruct.build(dict(
                    id=data.id,
                    size=data.size,
                    mtime=data.mtime,
                    path=data.path if data.path else ''
                ))

            case Protocol.ERROR_EVENT:
                assert data is not None, "ERROR_EVENT requires event data"
                assert isinstance(data, Types.ErrorEventTuple), "ERROR_EVENT requires ErrorEventTuple data"

                return msg + Types.ErrorStruct.build(dict(
                    event=data.event,
                    path=data.path if data.path else ''
                ))

            case _:
                assert False, f"Unknown cmd '{cmd}' detected"

    @staticmethod
    def parse_packet(packet: bytes) -> Dict[str, Any]:
        """ Deconstruct a command packet back into its parts """

        cmd = Types.CommandStruct.parse(packet)
        data: dict = None

        # the message payload will be different for each command

        match cmd['cmd']:
            case Protocol.SOURCE_EVENT:
                data = Types.EventStruct.parse(packet[2:])

            case Protocol.SOURCE_RESPONSE_FILE:
                data = Types.ResponseStruct.parse(packet[2:])

            case Protocol.SINK_FILESYSTEM_DELTA:
                data = Types.SyncStruct.parse(packet[2:])

            case Protocol.SINK_REQUEST_FILE:
                data = Types.RequestStruct.parse(packet[2:])

            case Protocol.ERROR_EVENT:
                data = Types.ErrorStruct.parse(packet[2:])

            case _:
                assert False, f"Unknown cmd '{cmd['cmd']}' detected"

        return cmd | data if isinstance(data, dict) else cmd
