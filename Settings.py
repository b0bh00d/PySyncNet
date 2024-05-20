#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Settings                                                        #
#                                                                         #
# Simple, generic class to hold settings.  This class is not thread-safe, #
# so settings should be static and established at startup                 #
#-------------------------------------------------------------------------#

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Settings(dict):
    # https://bobbyhadz.com/blog/python-use-dot-to-access-dictionary
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __contains__ = dict.__contains__
