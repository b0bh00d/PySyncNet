#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: Watcher                                                         #
#                                                                         #
# Watches the file system (using watchdog), reporting events using the    #
# signaling library                                                       #
#-------------------------------------------------------------------------#

import os
import stat
import signaling    # https://pypi.org/project/signaling/

import Types

from Events     import Event
from Settings   import Settings

# https://python-watchdog.readthedocs.io/en/stable/
from watchdog.observers import Observer     # https://pypi.org/project/watchdog/
from watchdog.events import PatternMatchingEventHandler

from loguru     import logger   # https://pypi.org/project/loguru/

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Watcher:
    """ Using watchdog, report any file system events that happend on (or under) the designated 'branch' """
    def __init__(self,
                 fs,#: FileSystem,
                 settings: Settings,
                 branch: str,
                 recursive: bool = True,
                 ignore_modified_folders: bool = True,
                 patterns: list = ["*"],
                 created_cb = None,
                 deleted_cb = None,
                 modified_cb = None,
                 renamed_cb = None):
        self._file_system = fs
        self._settings = settings
        self._branch: str = branch
        self._recursive: bool = recursive
        self._ignore_modified_folders: bool = ignore_modified_folders

        ignore_patterns: list[str] = None
        ignore_directories: bool = False
        case_sensitive: bool = True

        # from watchdog.observers import Observer
        # from watchdog.events import FileSystemEvent, PatternMatchingEventHandler
        # handler = PatternMatchingEventHandler(["*"], None, False, True)
        # handler.on_created = lambda event: print(f"Created {event.src_path}")
        # handler.on_deleted = lambda event: print(f"Deleted {event.src_path}")
        # handler.on_modified = lambda event: print(f"Modified {event.src_path}")
        # handler.on_moved = lambda event: print(f"Moved {event.src_path} -> {event.dest_path}")
        # observer = Observer()
        # observer.schedule(handler, "/tmp/SourcePictures", recursive=True)
        # observer.start()

        self._handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
        self._handler.on_created = self._on_created
        self._handler.on_deleted = self._on_deleted
        self._handler.on_modified = self._on_modified
        self._handler.on_moved = self._on_renamed

        self._observer = None

        if created_cb:
            self.created_signal = signaling.Signal(args=['event', 'size', 'mtime', 'path1', 'path2'])
            self.created_signal.connect(created_cb)
        if deleted_cb:
            self._deleted_signal = signaling.Signal(args=['event', 'size', 'mtime', 'path1', 'path2'])
            self._deleted_signal.connect(deleted_cb)
        if modified_cb:
            self._modified_signal = signaling.Signal(args=['event', 'size', 'mtime', 'path1', 'path2'])
            self._modified_signal.connect(modified_cb)
        if renamed_cb:
            self._renamed_signal = signaling.Signal(args=['event', 'size', 'mtime', 'path1', 'path2'])
            self._renamed_signal.connect(renamed_cb)

    def __del__(self) -> None:
        if self._observer:
            self._observer.stop()
            self._observer.join()

    def start(self) -> None:
        self._observer = Observer()
        self._observer.schedule(self._handler, self._branch, recursive=self._recursive)

        logger.info("Watcher started.")
        self._observer.start()

    def stop(self) -> None:
        if self._observer:
            self._observer.stop()
            self._observer.join()

            self._observer = None

    def _on_created(self, event) -> None:
        size: int = 0
        mtime: int = 0

        is_symlink: bool = os.path.islink(event.src_path)
        is_dir: bool = event.is_directory
        if is_dir:
            self._file_system.add_folder(event.src_path)
        else:
            file: Types.File = self._file_system._find(event.src_path)
            st = os.stat(event.src_path) if file is None else file.stat
            size = st.st_size
            mtime = int(st.st_mtime)
            self._file_system.add_file(event.src_path)

        event_type: Event = Event.CREATED_FOLDER if event.is_directory else Event.CREATED_FILE
        event_type = Event.CREATED_SYMLINK if is_symlink else event_type
        p1: str = event.src_path
        p2: str = None

        # if this is a symlink, get the target as path2
        if is_symlink:
            p2 = os.path.join(self._branch, os.readlink(p1))

        self.created_signal.emit(event=event_type, size=size, mtime=mtime, path1=p1, path2=p2)

    def _on_deleted(self, event) -> None:
        is_symlink: bool = False
        if not event.is_directory:
            file: Types.File = self._file_system._find(event.src_path)
            is_symlink: bool = stat.S_ISLNK(file.stat.st.st_mode)

        event_type = Event.DELETED_FOLDER if event.is_directory else Event.DELETED_FILE
        if is_symlink:
            event_type = Event.DELETED_SYMLINK

        self._deleted_signal.emit(event=event_type,size=0, mtime=0,path1=event.src_path, path2=None)
        self._file_system.del_item(event.src_path)

    def _on_modified(self, event) -> None:
        size: int = 0
        mtime: int = 0

        if not event.is_directory:
            self._file_system.update_file(event.src_path)

            file: Types.File = self._file_system._find(event.src_path)
            size = file.stat.st_size
            mtime = int(file.stat.st_mtime)

        if (not event.is_directory) or (not self._ignore_modified_folders):
            self._modified_signal.emit(event=Event.MODIFIED_FILE,
                                       size=size,
                                       mtime=mtime,
                                       path1=event.src_path, path2=None)

    def _on_renamed(self, event) -> None:
        if self._file_system.move_item(event.src_path, event.dest_path):
            self._renamed_signal.emit(event=Event.RENAMED_FOLDER if event.is_directory else Event.RENAMED_FILE,
                                    size=0, mtime=0,
                                    path1=event.src_path, path2=event.dest_path)
