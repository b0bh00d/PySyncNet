from __future__ import annotations
# ^^ allow class names to be referenced as types in class methods

#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Types                                                           #
#                                                                         #
# Type defines used throughout the system                                 #
#-------------------------------------------------------------------------#

import os
import construct    # https://pypi.org/project/construct/
import collections

try:
    from dataclasses import dataclass
except Exception:
    raise Exception('SyncNet requires python 3.7+')

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

# buffer size
BUFFER_SIZE = 1024*1024

# this is the data type created when an event happens on the Source's file system
SourceEventTuple = collections.namedtuple('SourceEventTuple', 'event size mtime path1 path2 ids')

# when a Sink notifies the Source about its file-system snapshot, this is the
# data that is provided to Protocol.message_packet()
SyncTuple = collections.namedtuple('SyncTuple', 'size')

# when a Sink needs a file update, it will send this packet
RequestTuple = collections.namedtuple('RequestTuple', 'id size mtime path')

# when a Source sends a file update, it will preface it with this packet
ResponseTuple = collections.namedtuple('ResponseTuple', 'id path')

# this is the data type created when an event happens on the Source's file system
ErrorEventTuple = collections.namedtuple('ErrorEventTuple', 'event path')

# contain the results of a diff between two FileSystems
DiffFolderTuple = collections.namedtuple('DiffFolderTuple', 'deleted added moved')
DiffFilesTuple = collections.namedtuple('DiffFilesTuple', 'deleted added modified moved resume')
DiffTuple = collections.namedtuple('DiffTuple', 'files folders')

# data structure used for event command packets
CommandStruct = construct.Struct(
    "cmd" / construct.Short,
    # bytes[]: payload
)
EventStruct = construct.Struct(
    "event" / construct.Short,
    "size" / construct.Int32ul,
    "mtime" / construct.Int32ul,
    "path1" / construct.PascalString(construct.VarInt, "utf-8"),
    "path2" / construct.PascalString(construct.VarInt, "utf-8"),
)
# a Sink will send this packet to request a file from the Source's file system
RequestStruct = construct.Struct(
    "id" / construct.Int32ul,       # request id from Sink (to identify this request)
    "size" / construct.Int32ul,     # size of the file when event occurred
    "mtime" / construct.Int32ul,    # mtime of the file when event occurred
    "path" / construct.PascalString(construct.VarInt, "utf-8"),
)
# a Source will send this packet to indicate that file data is incoming
ResponseStruct = construct.Struct(
    "id" / construct.Int32ul,       # request id from Sink (to identify this request)
    "path" / construct.PascalString(construct.VarInt, "utf-8"),
)
# a Sink will send this packet to let the Source know its file-system snapshot is coming
SyncStruct = construct.Struct(
    # "id" / construct.Int32ul,     # send id; will be included in each data packet
    "size" / construct.Int32ul,     # number of bytes to expect
)
# data structure used for string value
ErrorStruct = construct.Struct(
    "event" / construct.Short,
    "path" / construct.PascalString(construct.VarInt, "utf-8"),
)

@dataclass
class Directory(dict):
    """ A POD structure used by the FileSystem to represent a directory entry """
    stat: os.stat_result = None
    count: int = 0
    fingerprint: int = 0

    def __eq__(self, other: Directory) -> bool:
        """ Compare our fingerprint to that of another Directory entry """
        assert isinstance(other, Directory), "Only identical types can be compared for equality"
        return self.fingerprint == other.fingerprint
        # return (self.stat.st_size == other.stat.st_size) and \
        #         (int(self.stat.st_mtime) == int(other.stat.st_mtime))

@dataclass
class File:
    """ A POD structure used by the FileSystem to represent a file entry """
    stat: os.stat_result = None
    symlink: bool = False
    fingerprint: bytes = None

    def __eq__(self, other: File) -> bool:
        """ Compare our fingerprint to that of another File entry """
        assert isinstance(other, File), "Only identical types can be compared for equality"
        return self.fingerprint == other.fingerprint
        # return (self.stat.st_size == other.stat.st_size) and \
        #         (int(self.stat.st_mtime) == int(other.stat.st_mtime))
