#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Events                                                          #
#                                                                         #
# Exposes enums for file system events                                    #
#-------------------------------------------------------------------------#

from enum import IntEnum

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Event(IntEnum):
    """ Event enums class """
    NONE = 0
    CREATED_FILE = 1
    CREATED_FOLDER = 2
    CREATED_SYMLINK = 3     # a symlink (to a file or folder) was created
    DELETED_FILE = 4        # would also include a symlink
    DELETED_FOLDER = 5
    DELETED_SYMLINK = 6
    MODIFIED_FILE = 7
    MODIFIED_FOLDER = 8     # this is only here for completeness; it's not actually used
    RENAMED_FILE = 9
    RENAMED_FOLDER = 10

    @staticmethod
    def is_created(event) -> bool:
        return (event == Event.CREATED_FILE) or (event == Event.CREATED_FOLDER) or (event == Event.CREATED_SYMLINK)

    @staticmethod
    def is_deleted(event) -> bool:
        return (event == Event.DELETED_FILE) or (event == Event.DELETED_FOLDER) or (event == Event.DELETED_SYMLINK)

    @staticmethod
    def is_modified(event) -> bool:
        return (event == Event.MODIFIED_FILE) or (event == Event.MODIFIED_FOLDER)

    @staticmethod
    def is_renamed(event) -> bool:
        return (event == Event.RENAMED_FILE) or (event == Event.RENAMED_FOLDER)
