#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Queue                                                           #
#                                                                         #
# Tracks the file system events reported by the Watcher module, re-       #
# prioritizing as indicated, and notifying change events on the head      #
#-------------------------------------------------------------------------#

import time

from typing         import List

import Types

from Events         import Event
from LinkedList     import DoubleLinkedList, Node
from Housekeeping   import Housekeeping

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class EventData(Node):
    """ This is a data node that holds information about a file system event """
    def __init__(self, event: Event, size: int, mtime: int, path1: str, path2: str = None, ids: list = []):
        super(Node, self).__init__()

        self.data = Types.SourceEventTuple(event, size, mtime, path1, path2, ids)

    def __eq__(self, node):
        return node.data.path1 == self.data.path1

class Queue:
    """ This class manages pending processing of events """
    def __init__(self):
        self._changes = {}
        self._queue = DoubleLinkedList()

        # we cache deletions (and separate them) so we can currate
        # the results
        self._deleted_folders: List[EventData] = []
        self._deleted_files: List[EventData] = []
        self._last_deletion_ns: int = 0

        # we cache moves (and separate them) so we can currate
        # the results
        self._moved_folders: List[EventData] = []
        self._moved_files: List[EventData] = []
        self._last_move_ns: int = 0

        hk = Housekeeping()
        hk.request_service(self._housekeeping, 1_000_000_000)

    def _housekeeping(self):
        now = time.time_ns()
        if (self._last_deletion_ns != 0) and ((now - self._last_deletion_ns) >= 10_000_000):
            self._process_deletion_events()
        if (self._last_move_ns != 0) and ((now - self._last_move_ns) >= 10_000_000):
            self._process_move_events()

    def _process_deletion_events(self):
        if len(self._deleted_folders) > 1:
            self._deleted_folders.sort(key=lambda a: a.data[3])
            ln = len(self._deleted_folders)
            i: int = 0
            ii = i + 1
            while ii < ln:
                if (self._deleted_folders[ii] is not None) and \
                    self._deleted_folders[ii].data[3].startswith(self._deleted_folders[i].data[3]):
                    self._deleted_folders[ii] = None
                else:
                    # move on to the next unique folder entry
                    i = ii
                ii += 1
        _deleted_folders = [item for item in self._deleted_folders if item is not None]

        self._deleted_files.sort(key=lambda a: a.data[3])
        ln = len(self._deleted_files)
        for folder in _deleted_folders:
            i: int = 0
            while i < ln:
                if (self._deleted_files[i] is not None) and \
                    self._deleted_files[i].data[3].startswith(folder.data[3]):
                    self._deleted_files[i] = None
                i += 1
        _deleted_files = [item for item in self._deleted_files if item is not None]

        # each entry in _deleted_folders and _deleted_files are unique
        # and unrelated. inject them into the queue...

        for item in _deleted_files:
            self._changes[item.data[3]] = item
            self._queue.append(item)

        for item in _deleted_folders:
            self._changes[item.data[3]] = item
            self._queue.append(item)

        self._deleted_folders = []
        self._deleted_files = []
        self._last_deletion_ns = 0

    def _process_move_events(self):
        if len(self._moved_folders) > 1:
            self._moved_folders.sort(key=lambda a: a.data[3])
            ln = len(self._moved_folders)
            i: int = 0
            ii = i + 1
            while ii < ln:
                if (self._moved_folders[ii] is not None) and \
                    self._moved_folders[ii].data[3].startswith(self._moved_folders[i].data[3]):
                    self._moved_folders[ii] = None
                else:
                    # move on to the next unique folder entry
                    i = ii
                ii += 1
        _moved_folders = [item for item in self._moved_folders if item is not None]

        self._moved_files.sort(key=lambda a: a.data[3])
        ln = len(self._moved_files)
        for folder in _moved_folders:
            i: int = 0
            while i < ln:
                if (self._moved_files[i] is not None) and \
                    self._moved_files[i].data[3].startswith(folder.data[3]):
                    self._moved_files[i] = None
                i += 1
        _moved_files = [item for item in self._moved_files if item is not None]

        # each entry in _deleted_folders and _deleted_files are unique
        # and unrelated. inject them into the queue...

        for item in _moved_files:
            self._changes[item.data[3]] = item
            self._queue.append(item)

        for item in _moved_folders:
            self._changes[item.data[3]] = item
            self._queue.append(item)

        self._moved_folders = []
        self._moved_files = []
        self._last_move_ns = 0

    def push_kw(self, **kwargs) -> None:
        """ This function is a callback for when events happen on the file system """
        self.push(kwargs['event'], kwargs['size'], kwargs['mtime'], kwargs['path1'], kwargs['path2'])

    def push(self, event: Event, size: int, mtime: int, path1: str, path2: str, ids: list = []) -> None:
        """ This function can be called manually to add pseudo-events to the queue """
        # is this path already in the queue
        if path1 in self._changes:
            # some event has already occurred on this path;
            # shuffle it back to the bottom of the deck...
            node = self._changes[path1]
            # if self._queue.is_head(node) and (self._head_event_signal is not None):
            #     # if the path where the event occurred is at the head
            #     # of the queue, it might actually be locked and active
            #     # in the system.  notify the system that the node is
            #     # now invalid and is being re-prioritized
            #     self._head_event_signal.emit(event=event, path1=path1, path2=path2)
            del self._changes[path1]
            self._queue.remove(node)

        node = EventData(event, size, mtime, path1, path2, ids)
        if Event.is_deleted(event):
            if event == Event.DELETED_FOLDER:
                self._deleted_folders.append(node)
            else:
                self._deleted_files.append(node)

            # make note of the time the last deletion was processed.
            # the timer function will use this to process pending
            # deletions if events have been quiet for some reasonable
            # timeout

            self._last_deletion_ns = time.time_ns()
        elif Event.is_renamed(event):
            if event == Event.RENAMED_FOLDER:
                self._moved_folders.append(node)
            else:
                self._moved_files.append(node)

            # make note of the time the last move was processed.
            # the timer function will use this to process pending
            # moves if events have been quiet for some reasonable
            # timeout

            self._last_move_ns = time.time_ns()
        else:
            # this is not a deletion or move event; inject any pending
            # deletions/moves before we process the current event
            if len(self._deleted_folders) or len(self._deleted_files):
                self._process_deletion_events()
            if len(self._moved_folders) or len(self._moved_files):
                self._process_move_events()
            self._changes[path1] = node
            self._queue.append(node)

    def pop(self) -> EventData:
        # assert not self._head_locked, "Calling Queue.pop while head is locked"
        node = self._queue.head()
        self._queue.pop_head()
        del self._changes[
            # this is not tight binding; the purpose of this module is to manage
            # a queue of events, and we construct the 'data' element of the node
            # ourselves,so we are already familiar with its elements
            node.data.path1
        ]

        # NOTE: caller should check the existence of node.data.path1,
        # because a DELETED event might have removed it entirely

        return node

    def is_empty(self) -> bool:
        return len(self._queue) == 0
