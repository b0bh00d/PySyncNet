#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Utilities                                                       #
#                                                                         #
# Various system-wide support utilities                                   #
#-------------------------------------------------------------------------#

import os
import locale

import Types

from MD5 import MD5

from typing import Any

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

def format_number(number) -> str:
    """ Format an integer value using a locale-specific display """
    items = list(number) if isinstance(number, str) else list(str(number))

    new_list = []
    offset = -1
    ln = len(items)
    sep = locale.localeconv()['thousands_sep']
    assert len(sep) != 0, "Locale has not been initialized"
    while ln > 0:
        new_list.insert(0, items[offset])
        if (ln > 1) and ((offset % 3) == 0):
            new_list.insert(0, sep)
        ln -= 1
        offset -= 1

    return ''.join(new_list)

def calculate_fingerprint(data: bytes, start: int = None, end: int = None) -> Any:
    m = MD5()
    if os.path.exists(data):
        # produce hash from file contents
        if start is None:
            start = 0
        if end is None:
            st = os.stat(data)
            end = st.st_size

        buffer_size = int(Types.BUFFER_SIZE)
        count = end - start

        try:
            with open(data, "rb") as f:
                while count != 0:
                    chunk = f.read(buffer_size if count >= buffer_size else count)
                    count -= len(chunk)
                    m.update(chunk)
        except Exception:
            raise
    else:
        # produce hash directly from byte buffer
        if start is not None:
            if start > len(data):
                raise Exception('')
        else:
            start = 0
        if end is not None:
            if end < 0 or end > len(data):
                raise Exception('')
        else:
            end = len(data)

        m.update(data[start:end])

    return m
