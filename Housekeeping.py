#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Housekeeping                                                    #
#                                                                         #
# Singleton manager for parties interested in having regular attention    #
# from the main loop time slice                                           #
#-------------------------------------------------------------------------#

import time
import asyncio
import functools

from typing     import Any, Callable

class Housekeeping(object):
    MODE_IMMEDIATE, MODE_ASYNC = range(2)

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Housekeeping, cls).__new__(cls)
            cls.instance._rooms = {}
            cls.instance._tasks = set()
        return cls.instance

    def _room_is_async(self, obj: Any) -> bool:
        while isinstance(obj, functools.partial):
            obj = obj.func

        return asyncio.iscoroutinefunction(obj) or \
            (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))

    def perform(self):
        now = time.time_ns()
        for room in self._rooms:
            if (now - self._rooms[room][1]) > self._rooms[room][0]:

              self._rooms[room][1] = now

              if self._rooms[room][2] == Housekeeping.MODE_ASYNC:
                  # schedule room service for asynchronous invocation
                  task = asyncio.create_task(room())
                  self._tasks.add(task)
                  task.add_done_callback(self._tasks.discard)

              elif self._rooms[room][2] == Housekeeping.MODE_IMMEDIATE:
                  # invoke room service immediately, blocking until complete
                  room()

    def request_service(self, interested: Callable, interval: int):
        # if a callable is already registered, this will update its
        # information
        self._rooms[interested] = [interval, time.time_ns(),
          Housekeeping.MODE_ASYNC if self._room_is_async(interested) else Housekeeping.MODE_IMMEDIATE]

    def do_not_disturb(self, interested: Callable):
        assert interested in self._rooms, "Callable not registered with Housekeeping"
        del self._rooms[interested]
