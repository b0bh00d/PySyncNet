import typing
import inspect
import asyncio
import functools

from signaling.exceptions import InvalidEmit, InvalidSlot, SlotNotAsync

class Signal(object):
    MODE_AUTO, MODE_IMMEDIATE, MODE_ASYNC = range(3)

    def __init__(self, args=None, name=None):
        self.slots = []
        self.name = name
        self.args = args

        # keep track of slot invocations that are currently active (async)
        self.active_slots = set()

    def __eq__(self, other):
        return self.slots == other.slots

    def __repr__(self):
        return u"<Signal: '{}'. Slots={}>".format(self.name or 'anonymous', len(self.slots))

    def __del__(self):
        # if there are any pending signals, cancel them
        for task in self.active_slots:
            task.cancel()

    def emit(self, **kwargs) -> None:
        """Emit signal by calling all connected slots.

        The arguments supplied have to match the signal definition.

        Args:
            kwargs: Keyword arguments to be passed to connected slots.

        Raises:
            :exc:`InvalidEmit`: If arguments don't match signal specification.
        """
        self._ensure_emit_kwargs(kwargs)
        for slot, mode in self.slots:
            if mode == Signal.MODE_ASYNC:
                # schedule the slot for asynchronous invocation
                task = asyncio.create_task(slot(**kwargs))
                self.active_slots.add(task)
                task.add_done_callback(self.active_slots.discard)

            elif mode == Signal.MODE_IMMEDIATE:
                # invoke the slot immediately, blocking until complete
                slot(**kwargs)

    def _slot_is_async(self, obj: typing.Any) -> bool:
        while isinstance(obj, functools.partial):
            obj = obj.func

        return asyncio.iscoroutinefunction(obj) or \
            (callable(obj) and asyncio.iscoroutinefunction(obj.__call__))

    def _ensure_emit_kwargs(self, kwargs) -> None:
        if self.args and set(self.args).symmetric_difference(kwargs.keys()):
            raise InvalidEmit(f"Emit has to be called with args '{self.args}'")
        elif not self.args and kwargs:
            raise InvalidEmit("Emit has to be called without arguments.")

    def is_connected(self, slot) -> bool:
        """Check if a slot is conncted to this signal."""
        return len([item for item in self.slots if item[0] == slot]) != 0

    def connect(self, slot, mode=MODE_AUTO) -> None:
        """Connect ``slot`` to this singal.

        Args:
            slot (callable): Callable object wich accepts keyword arguments.

        Raises:
            InvalidSlot: If ``slot`` doesn't accept keyword arguments.
            SlotNotAsync: If MODE_ASYNC is specified and ``slot`` is not a coroutine.
        """
        self._ensure_slot_args(slot)
        if not self.is_connected(slot):

            match mode:
                case Signal.MODE_AUTO:
                    mode = Signal.MODE_ASYNC if self._slot_is_async(slot) \
                        else Signal.MODE_IMMEDIATE
                case Signal.MODE_ASYNC:
                    if not self._slot_is_async(slot):
                        raise SlotNotAsync(
                            "Slot used for connection is not async.")
                case _:
                    mode = Signal.MODE_IMMEDIATE

            self.slots.append((slot, mode))

    def _ensure_slot_args(self, slot) -> None:
        argspec = inspect.getfullargspec(slot)
        if inspect.ismethod(slot) and 'self' in argspec.args:
            argspec.args.remove('self')
        if self.args and self.args != argspec.args and not argspec.varkw:
            raise InvalidSlot(
                f"Slot '{slot.__name__}' has to accept args {self.args} or **kwargs.")
        if not self.args and argspec.args:
            raise InvalidSlot(
                f"Slot '{slot.__name__}' has to be callable without arguments")

    def disconnect(self, slot) -> None:
        """Disconnect ``slot`` from this signal."""
        if self.is_connected(slot):
            self.slots = [item for item in self.slots if item[0] != slot]
            # self.slots.remove(slot)
