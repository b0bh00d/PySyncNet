class SignalSlotException(Exception):
    """ Base signal/slot exception """

class InvalidSlot(SignalSlotException):
    """ Indicates that the slot implementation is invalid """

class InvalidEmit(SignalSlotException):
    """ Indicates that the emit method was called with invalid arguments """

class SlotNotAsync(SignalSlotException):
    """ A slot was explicitly flagged with MODE_ASYNC, but could not be confirmed as a coroutine """
