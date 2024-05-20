#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: MD5                                                             #
#                                                                         #
# An MD5 hash implementation that can be persisted and restored using     #
# pickle.                                                                 #
# Note: This uses a native-language extension for speed                   #
#                                                                         #
# "Why on Earth would you want to do this?"                               #
#   To head off this inevitible question, the md5() implementation in     #
#   hashlib has one problematic flaw (at least for my purposes): It's     #
#   current state cannot be serialized such that you can resume later.    #
#   I need that functionality for SyncNet to aid in the avoidance of      #
#   having to restart the transfer of potentially massive files (tens or  #
#   hundreds of gigabytes in size).  This implemenation makes that        #
#   possible, and it is actaully almost as fast (in my testing).          #
#-------------------------------------------------------------------------#

import snmd5

from typing import TypeVar

AnyStr = TypeVar('AnyStr', bound=str|bytes)

# https://docs.python.org/3/extending/extending.html
# https://nexwebsites.com/blog/cpp-python-extensions/
class MD5:
    def __init__(self):
        self._context = snmd5.new()

    def __getstate__(self) -> AnyStr:
        return self.getState()

    def __setstate__(self, value: AnyStr) -> None:
        self.setState(value)

    def update(self, message: AnyStr) -> None:
        snmd5.update(self._context, message)

    def getState(self) -> bytes:
        context = snmd5.getState(self._context)
        assert context is not None, "snmd5 context is None"
        return context

    def setState(self, context: AnyStr) -> None:
        assert snmd5.setState(context), "failed to restore snmd5 state"

    def digest(self) -> int:
        return snmd5.snapshot(self._context)

    def hexdigest(self) -> str:
        raw = snmd5.snapshot(self._context)
        return '{:032x}'.format(int.from_bytes(raw, byteorder='big'))
