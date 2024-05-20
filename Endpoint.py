#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Endpoint                                                        #
#                                                                         #
# Base class for the Source and Sink subclasses.  Handles signal          #
# interrupts, and creates a contract for the execute() method             #
#-------------------------------------------------------------------------#

import uuid
# import signal

from Settings   import Settings

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Endpoint:
    TYPE_SOURCE, TYPE_SINK = range(2)

    def __init__(self, settings: Settings, type: int=TYPE_SINK):
        self._settings = settings
        self._loop: bool = True
        self._uuid: str = uuid.uuid4().hex
        self._type: int = type

        # signal.signal(signal.SIGHUP, self._sighandler)
        # signal.signal(signal.SIGINT, self._sighandler)
        # signal.signal(signal.SIGQUIT, self._sighandler)
        # signal.signal(signal.SIGILL, self._sighandler)
        # signal.signal(signal.SIGABRT, self._sighandler)
        # signal.signal(signal.SIGSEGV, self._sighandler)
        # signal.signal(signal.SIGTERM, self._sighandler)

    # def _sighandler(self, signal, frame) -> None:
    #     self._loop = False

    async def execute() -> None:
        raise Exception("Invoking base class method")

    def is_source(self) -> bool:
        return self._type == Endpoint.TYPE_SOURCE

    def is_sink(self) -> bool:
        return self._type == Endpoint.TYPE_SINK

    @property
    def id(self) -> str:
        return self._uuid

    @property
    def type(self) -> int:
        return self._type
