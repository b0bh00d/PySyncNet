#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: FileSystem                                                      #
#                                                                         #
# Virtual file system tracker, bespoke for the needs of SyncNet           #
#-------------------------------------------------------------------------#

import os
import sys
# import lzma
# import pickle
import signaling    # https://pypi.org/project/signaling/

try:
    from dataclasses import dataclass
except Exception:
    raise Exception('SyncNet requires python 3.7+')

from typing     import Dict, Tuple, List, AnyStr, BinaryIO, TypeVar

import Types

from Queue      import Queue
from Watcher    import Watcher
from Settings   import Settings
from Events     import Event
from Utilities  import calculate_fingerprint
from MD5        import MD5

from loguru     import logger   # https://pypi.org/project/loguru/

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class FileSystem:
    # these constants define the temporary file extensions used by a Sink
    # when receiving file data from a Source,  files with the PART_EXT
    # value may appear in the FileSystem reference when a Sink attempts
    # to synchronize file systems. the FileSystem will detect them, and
    # determine if the transfer can be resumed

    PART_EXT = ".sn_part"
    META_EXT = ".sn_neta"

    @dataclass
    class Counts:
        folders: int = 0
        files: int = 0

    FindResult = TypeVar('FindResult', Types.File, Types.Directory, None)

    class FileStream:
        """
        Manages a file stream, making its contents avaialble while also monitoring its validity.
        The "file stream" can be either an actual file system entity, or it can be a buffer of
        bytes:  The user won't know the difference.
        """
        class EndOfFile(Exception):
            pass

        class Invalid(Exception):
            pass

        def __init__(self, parent, data: bytes):
            super(FileSystem.FileStream, self).__init__()

            self._parent: FileSystem = parent
            self._buffer: bytes = data
            self._buffer_len = len(data)
            self._buffer_index: int = 0

            self._file: BinaryIO = None
            self._is_file: bool = False
            self._is_valid: bool = True
            self._is_eof: bool = False

            if os.path.exists(data):
                self._is_file = True
                try:
                    self._file = open(data, 'rb')
                except Exception:
                    self._file = None
                    self._is_valid = False
                    self._is_eof = True

        def __del__(self):
            if self._file is not None:
                self._file.close()
                self._file = None
            self._parent.release_stream(self)

        def _on_invalidation_event(self, **kwargs) -> None:
            # if we get invoked, regardless of the event type, the file
            # we're managing has become invalid
            self._is_valid = False
            if self._file is not None:
                self._file.close()
                self._file = None

        async def read(self, buffer_size: int) -> bytes:
            if not self._is_file:
                if self._is_eof:
                    raise FileSystem.FileStream.EndOfFile("File stream has reached the end")
                else:
                    available = self._buffer_len - self._buffer_index
                    if available <= buffer_size:
                        self._is_eof = True
                        return self._buffer[self._buffer_index:]
                    data = self._buffer[self._buffer_index:self._buffer_index + buffer_size]
                    self._buffer_index += buffer_size
                    return data
            else:
                if not self._is_valid:
                    raise FileSystem.FileStream.Invalid("An invalidating event has occurred")
                elif self._is_eof:
                    raise FileSystem.FileStream.EndOfFile("File stream has reached the end")
                else:
                    data: bytes = b''

                    if self._file is not None:
                        data = self._file.read(buffer_size)
                        self._is_eof = len(data) < buffer_size

                    return data

        @property
        def is_file(self) -> bool:
            return self._is_file

        @property
        def id(self) -> bytes:
            return self._buffer

        @property
        def eof(self) -> bool:
            """ Report on the state of the input stream """
            return self._is_eof

        @property
        def valid(self) -> bool:
            """ Return a value that indicates whether or not the data is out of date (modified) """
            return self._is_valid

        @valid.setter
        def valid(self, value) -> None:
            if not isinstance(value, bool):
                raise ValueError("Valid must be a Boolean type")
            self._is_valid = value

    def __init__(self, settings: Settings, branch: str, id: int):
        self._settings = settings
        self._id = id
        self._branch: str = branch if not branch.endswith('/') else branch[:-1]
        self._counts = FileSystem.Counts(folders=0, files=0)
        self._watcher: Watcher = None
        self._active_streams: Dict[str, signaling.Signal] = {}
        self._entries_to_remove: str = []

        def cache_state(path, data) -> None:
            key = self._path_to_relative(path)
            if key != '.':
                key = os.path.split(path)[-1]
            data[key] = Types.Directory(stat=os.stat(path), count=0, fingerprint=0)

            try:
                # some items we may not be able to access (like 'lost+found')
                items = os.listdir(path)
            except PermissionError:
                items = None

            if items is not None:
                for item in items:
                    full_path = os.path.join(path, item)
                    if not os.path.isdir(full_path):
                        data[key][item] = Types.File(stat=None, symlink=False, fingerprint=0)
                        # self._update_fingerprint(full_path)
                        self._counts.files += 1
                for item in items:
                    full_path = os.path.join(path, item)
                    if os.path.isdir(full_path):
                        if os.path.islink(full_path):
                            # treat it as a file instead of a directory
                            data[key][item] = Types.File(stat=None, symlink=False, fingerprint=0)
                            self._counts.files += 1
                        else:
                            self._counts.folders += 1
                            cache_state(full_path, data[key])
                    # self._update_fingerprint(full_path)

        # create an initial snapshot of the file syst6em we need to track
        logger.info(f"Imaging root path {self._branch}...")

        self._root = {}
        cache_state(self._branch, self._root)
        self._update_fingerprint(self._branch, recurse=True)

        # https://stackoverflow.com/questions/57983431/whats-the-most-space-efficient-way-to-compress-serialized-python-data
        # with open('/tmp/filesystem.pickle', 'wb') as f:
        #     pickle.dump(self, f)

        # with lzma.open("/tmp/filesystem_lzma.pickle", "wb") as f:
        #     pickle.dump(self, f)

        # sys.exit(0)

        # clear any enries that update_fingerprint() identified
        # as invalid...
        for entry in self._entries_to_remove:
            self.del_item(entry)

    def __del__(self):
        if self._watcher is not None:
            self.ignore_events()

    def __getstate__(self):
        """ Adjust our copy to make it pickle properly """
        state = self.__dict__.copy()
        # remove '_settings' because (a) they won't apply on the
        # other end, and (b) they won't pickle
        del state['_settings']
        return state

    def __setstate__(self, state):
        """ Restore our state from a sanitized pickle """
        self.__dict__.update(state)

    # implement the 'in' operator
    def __contains__(self, full_path: str) -> bool:
        path = self._path_to_relative(full_path)
        p, f = os.path.split(path)
        items = p.split('/')
        point = self._root
        for key in items:
            try:
                point = point[key]
            except KeyError:
                break
        return f in point

    def _update_fingerprint(self, full_path: str, recurse: bool = False) -> None:
        """ Calculate an MD5 fingerprint for each path in the file system """
        # key = full_path.split('/')[-1]
        entry: FileSystem.FindResult = self._find(full_path)
        assert entry is not None, f"Could not find '{full_path}' in the FileSystem database"
        if recurse:
            assert isinstance(entry, Types.Directory), "Recursion can only be done with a Directory object"

        is_link = os.path.islink(full_path)

        m1 = MD5()
        if (not is_link) and isinstance(entry, Types.Directory):
            entry.count = len(entry)

            # each folder has an MD5 value, calculated using the MD5 fingerprints
            # of its immediate children, while it is a member in the tree.  this
            # serves as a snapshot of the folder's current state, and is used when
            # calculating diffs to attempt to detect if a file/folder has been
            # moved (offline)

            for item in entry:
                if recurse and isinstance(entry[item], Types.Directory):
                    self._update_fingerprint(os.path.join(full_path, item), recurse)

                # we sum the MD5 for each entry in this folder, assign it
                # to the entry, and then add it to the MD5 sum for the folder

                # item_path = os.path.join(full_path, item)
                # s = os.stat(item_path)
                # ss = f"{item}{s.st_size}{s.st_mtime}{s.st_mode}{s.st_uid}{s.st_gid}{s.st_nlink}"

                # m2 = MD5()
                # m2.update(ss.encode('utf-8'))

                m2 = MD5()

                m2.update(item.encode('utf-8'))
                item_path = os.path.join(full_path, item)
                s = os.stat(item_path)
                # we don't use the entirety of the stat result, because
                # it can contain location-specific data (like st_ino) which
                # might change if it is moved
                m2.update(int(s.st_size).to_bytes(8, 'little', signed=False))
                m2.update(int(s.st_mtime).to_bytes(8, 'little', signed=False))
                m2.update(int(s.st_mode).to_bytes(8, 'little', signed=False))
                m2.update(int(s.st_uid).to_bytes(8, 'little', signed=False))
                m2.update(int(s.st_gid).to_bytes(8, 'little', signed=False))
                m2.update(int(s.st_nlink).to_bytes(8, 'little', signed=False))

                entry[item].fingerprint = m2.digest()

                m1.update(entry[item].fingerprint)

        elif is_link or isinstance(entry, Types.File):
            if FileSystem.PART_EXT in full_path:
                # this is a partial download (will only appear on Sink
                # FileSystems).  the "fingerprint" for this file is stored
                # in the META_EXT file that should accompany it.  if there
                # is no META_EXT file, and its current "fingerprint" is zero,
                # then log a warning and remove it from the system

                len_ext = len(FileSystem.PART_EXT)
                base_file_name = full_path[:-len_ext]
                meta_file_name = base_file_name + FileSystem.META_EXT

                to_be_removed: str = None

                if entry.fingerprint is None:
                    if not os.file.exists(meta_file_name):
                        logger.warning(f"'{full_path}' missing metadata info; deleting")
                        to_be_removed = full_path
                    else:
                        with open(meta_file_name, "rb") as f:
                            m3 = MD5()
                            m3.setState(f.read())
                            entry.fingerprint = m3.snapshot()
                        to_be_removed = meta_file_name
                elif os.file.exists(meta_file_name):
                    to_be_removed = meta_file_name

                if to_be_removed is not None:
                    try:
                        os.remove(to_be_removed)
                        self._entries_to_remove.append(to_be_removed)
                    except Exception:
                        logger.error(f"Failed to remove file '{to_be_removed}'")

            entry.symlink = is_link
            rel_path = self._path_to_relative(full_path)
            m1.update(rel_path.encode('utf-8'))
            entry.stat = os.lstat(full_path) if is_link else os.stat(full_path)
            m1.update(int(entry.stat.st_size).to_bytes(8, 'little', signed=False))
            m1.update(int(entry.stat.st_mtime).to_bytes(8, 'little', signed=False))
            m1.update(int(entry.stat.st_mode).to_bytes(8, 'little', signed=False))
            m1.update(int(entry.stat.st_uid).to_bytes(8, 'little', signed=False))
            m1.update(int(entry.stat.st_gid).to_bytes(8, 'little', signed=False))
            m1.update(int(entry.stat.st_nlink).to_bytes(8, 'little', signed=False))

        entry.fingerprint = m1.digest()

    def _match_fingerprint(self, full_path: str, count: int, fingerprint: bytes) -> bool:
        m = calculate_fingerprint(full_path, 0, count)
        return m.digest() == fingerprint

    def _find(self, full_path: str) -> FindResult:
        path = self._path_to_relative(full_path)
        items = path.split('/')
        point = self._root
        for key in items:
            try:
                point = point[key]
            except KeyError:
                point = None
                break
        return point

    def _path_to_absolute(self, path: str) -> str:
        """ Convert a relative path to an aboslute path on our branch """
        if not os.path.isabs(path):
            assert path.startswith('./'), f"Provided path {path} does not use an expected format"
            return path.replace('./', self._branch)
        return path

    def _path_to_relative(self, path: str) -> str:
        """ Convert an absolute path on our branch to a relative path """
        if os.path.isabs(path):
            assert path.startswith(self._branch), f"Provided path {path} does not use an expected format"
            return path.replace(self._branch, '.')
        return path

    def _invalidating_event(self, **kwargs) -> None:
        """ This function is a callback for when an invalidating event happens on a system element """

        # only files that have content will be in active_streams{}, so
        # we don't need to sift for directories or symlinks

        if kwargs['path1'] in self._active_streams:
            # a file that has been changed is being actively transferred to a client
            self._active_streams[kwargs['path1']].emit(kwargs['event'])

        # forward the event to the Queue
        self._queue.push(kwargs['event'], kwargs['size'], kwargs['mtime'], kwargs['path1'], kwargs['path2'])

    def monitor_events(self,
              recursive: bool = True,
              ignore_modified_folders: bool = True,
              patterns: list = ["*"]) -> Queue:
        """ Initiates monitoring of file-system events """
        assert self._watcher is None, "Watcher.report_events() called while Watcher instance is already active"

        # Queue will capture the events generated by Watcher
        self._queue = Queue()

        # Watcher will report file system events into Queue
        self._watcher = Watcher(self,
                                self._settings,
                                self._branch,
                                recursive,
                                ignore_modified_folders,
                                patterns,
                                self._invalidating_event,   # created
                                self._queue.push_kw,        # deleted
                                self._invalidating_event,   # modified
                                self._queue.push_kw)        # renamed

        # start paying attention to events
        self._watcher.start()

        # let the caller retrieve file-system events directly from the Queue
        return self._queue

    def ignore_events(self) -> None:
        """ Suspends monitoring of file-system events """
        assert self._watcher, "Watcher.ignore_events() called before Watcher instance was created"
        self._watcher.stop()
        self._watcher = None
        self._queue = None

    def print_fs(self) -> None:
        def process_fs(data, folder, indent):
            print('{}{}/'.format('  ' * indent, folder))
            items = data[folder]
            for key in items:
                if isinstance(items[key], Types.File):
                    print('{}{}'.format('  ' * (indent + 1), key))
                elif isinstance(items[key], Types.Directory):
                    process_fs(items, key, indent + 1)

        indent = 0
        process_fs(self._root, ".", indent)

    def add_folder(self, full_path: str) -> bool:
        """ Update the memory cache with the addition of a new folder """
        path = full_path.replace(self._branch, '.')
        incr_path = self._branch
        update_paths = []
        p, f = os.path.split(path)
        items = p.split('/')
        point = self._root
        for item in items:
            if item != '.':
                incr_path = os.path.join(incr_path, item)
            if item not in point:
                point[item] = Types.Directory(stat=os.stat(incr_path), count=0, fingerprint=0)
                self._counts.folders += 1
                update_paths.append(incr_path)
            point = point[item]
        # logger.info(f"Adding {path}/")
        incr_path = os.path.join(incr_path, f)
        point[f] = Types.Directory(stat=None, count=0, fingerprint=0)
        update_paths.append(incr_path)
        self._counts.folders += 1

        for path in update_paths:
            self._update_fingerprint(path)

        return True

    def move_item(self, src_full_path: str, dest_full_path: str) -> bool:
        """ Rename an existing file-system item """

        # see if the target already exists--if so, then
        # this is likely part of a previous parental move
        dest_path = self._path_to_relative(dest_full_path)
        p, f = os.path.split(dest_path)
        items = p.split('/')
        point = self._root
        try:
            for item in items:
                point = point[item]
        except KeyError:
            pass

        if f not in point:
            src_path = self._path_to_relative(src_full_path)
            p, f = os.path.split(src_path)
            items = p.split('/')
            point = self._root
            for item in items:
                point = point[item]

            val = point[f]
            del point[f]

            # find the target location
            p, f = os.path.split(dest_path)
            items = p.split('/')
            point = self._root
            for item in items:
                point = point[item]

            point[f] = val

            self._update_fingerprint(src_full_path)
            self._update_fingerprint(dest_full_path)

            return True

        return False

    def del_item(self, full_path: str) -> bool:
        """
        Remove a file-system item from the memory cache

        Note: Deletion is an invalidating event, and will trigger a signal
        """
        if full_path in self._active_streams:
            # this should trigger a subsequent call to release_stream()
            self._active_streams[full_path].emit(event=Event.DELETED_FILE)

        path = self._path_to_relative(full_path)
        p, f = os.path.split(path)
        items = p.split('/')
        point = self._root
        for item in items:
            point = point[item]

        try:
            is_file = isinstance(point[f], Types.File)

            del point[f]
            # logger.info(f"Deleted {path}")

            if is_file:
                self._counts.files -= 1
            else:
                self._counts.folders -= 1

            self._update_fingerprint(full_path)

        except KeyError:
            logger.warning(f"Path {path} is not in the database")
            return False

        return True

    def add_file(self, full_path: str) -> bool:
        """ Make note in the memory cache of a new file """
        path = self._path_to_relative(full_path)
        incr_path = self._branch
        update_paths = []
        p, f = os.path.split(path)
        items = p.split('/')
        point = self._root
        for item in items:
            if item != '.':
                incr_path = os.path.join(incr_path, item)
            if item not in point:
                point[item] = Types.Directory(stat=os.stat(incr_path), count=0, fingerprint=0)
                self._counts.folders += 1
                update_paths.append(incr_path)
            point = point[item]
        # logger.info(f"Adding {path}")

        update_paths.append(incr_path)

        try:
            point[f] = Types.File(stat=None, symlink=False, fingerprint=0)

            # there will be a subsequent UPDATE event for this; we'll update metadata there
            # self._update_metadata(full_path)

            self._counts.files += 1

            for path in update_paths:
                self._update_fingerprint(path)

        except FileNotFoundError:
            logger.warning(f"Path {path} no longer exists")

        return f in point

    def update_file(self, full_path: str) -> bool:
        """
        Update memory cache info for an existing branch file

        Note: Update is an invalidating event, and will trigger a signal
        """
        if full_path in self._active_streams:
            # this should trigger a subsequent call to release_stream()
            self._active_streams[full_path].emit(event=Event.MODIFIED_FILE)

        path = self._path_to_relative(full_path)

        if not os.path.exists(full_path):
            logger.warning(f"Path {path} no longer exists")
            return False

        self._update_fingerprint(full_path)

        return True

    async def diff(self, fs) -> Types.DiffTuple:
        """ Produce a difference between two FileSystem instances """

        # steps:
        # 1. walk the current database first
        #   - anything found in it that is not in the source fs is an addition
        #   - each file that is found gets its stat object compared
        # 2. walk the previous fs
        #   - anything there that is not in the current database is a deletion

        def scan_fs(data: dict,
                    target: FileSystem,
                    folder: str,
                    full_path: str,
                    added_folders: List[AnyStr],
                    added_files: List[Tuple[AnyStr, int, int]],
                    modified_files: List[Tuple[AnyStr, int, int]] = None) -> None:
            full_path: str = os.path.join(full_path, folder)
            f_: Types.Directory = target._find(full_path)
            if f_ is None:
                added_folders.append(full_path)
            items = data[folder]
            for key in items:
                if isinstance(items[key], Types.File):
                    item_path: str = os.path.join(full_path, key)
                    if item_path not in target:
                        added_files.append((item_path, items[key]))
                    elif modified_files is not None:
                        target_item: Types.File = target._find(item_path)
                        # compare fingerprints
                        if items[key] != target_item:
                            modified_files.append((item_path, items[key]))
                elif isinstance(items[key], Types.Directory):
                    scan_fs(items, target, key, full_path, added_folders, added_files, modified_files)

        # we don't track "deletions", per se.  processing each file system
        # only detects additions and modifications compared to the other.
        # deletions are simply the additions logged by the previous file system
        # against the current file system--i.e., if a file or folder exists
        # in the old file system but not the new, that's an addition from the
        # previous file system's perspective, but a deletion from the current

        added_folders = []
        added_files = []
        modified_files = []
        scan_fs(self._root, fs, ".", "", added_folders, added_files, modified_files)

        # IN FACT, we can completely ignore "modifications" when scanning
        # the previous file system, because they will always be false--i.e.,
        # no entries in the previous file system will be considered newer
        # than the current

        deleted_folders = []
        deleted_files = []
        scan_fs(fs._root, self, ".", "", deleted_folders, deleted_files)

        # scan through the deleted_files[] and see if any are META files--these
        # will indicate an incomplete transfer on the Sink side.  Finding one,
        # we will compare it's fingerprint (which is the accumulated MD5 of the
        # bytes received) against the same byte offset in our local file.  if
        # they match, we will consider this a RESUME event; if not, we remove
        # it and add the base file to MODIFIED instead so it starts the
        # transfer again.

        resumeable_files = []
        new_deleted_files = []

        for entry in deleted_files:
            dpath, dfile = entry
            if FileSystem.META_EXT in dpath:
                base_name = dpath[:-len(FileSystem.META_EXT)]
                full_name = self._path_to_absolute(base_name)
                fp = calculate_fingerprint(full_name, end=dfile.stat.st_size)

                if fp == dfile.fingerprint:
                    # we can resume sending this file; remove the meta file from
                    # 'deleted', and add the base file to 'resume'
                    resumeable_files.append(entry)
                else:
                    # leave it in deleted_files[]

                    # this file has changed since we last sent data.
                    # the base file should already be in the MODIFIED
                    # list, and the Sink will notice a lingering META
                    # file and replace it when the file starts
                    # transferring again (to be tested)

                    pass
            else:
                # filter out META files
                new_deleted_files.append(entry)

        deleted_files = new_deleted_files
        new_deleted_files = None

        # attempt to detect (offline) moves using fingerprints.
        # NOTE: This will only detect "pure" moves--if a file or folder
        # is moved, and then subsequently modified, then its fingerprint
        # will no longer match the other file system, and a DELETE or
        # bandwidth-consuming ADD event will be necessary.

        # check folders first.  we gather all folder fingerprints from
        # both FileSystems, and then collate them to detect a folder
        # move.  for every moved folder we discover, we remove its children
        # from the deleted/added lists

        def gather_fingerprints(prints: Dict[bytes, List[str]], entries: Types.Directory, path: str):
            # note: fingerprints may have duplicates in the file system (e.g., a folder
            # copy on a different path),  we therefore must account for collisions
            if entries.fingerprint not in prints:
                prints[entries.fingerprint] = []
            prints[entries.fingerprint].append(path)
            for item in entries:
                if isinstance(entries[item], Types.Directory):
                    gather_fingerprints(prints, entries[item], os.path.join(path, item))

        self_fingerprints: Dict[bytes, List[str]] = {}
        gather_fingerprints(self_fingerprints, self._root['.'], '.')
        other_fingerprints: Dict[bytes, List[str]] = {}
        gather_fingerprints(other_fingerprints, fs._root['.'], '.')

        moved_folders = []

        prefixes_to_remove = []
        for fp in self_fingerprints:
            if fp in other_fingerprints:
                # if the path arrays under each fingerprint differs at all, then
                # something happened: it could be a move, an add or a deletion...
                if self_fingerprints[fp] != other_fingerprints[fp]:
                    # ...however, we are only interested in moves here, so if
                    # 1. both fingerprints have only one entry, it's a move

                    if (len(self_fingerprints[fp]) == 1) and (len(other_fingerprints[fp]) == 1):
                        self_path = self_fingerprints[fp][0]
                        other_path = other_fingerprints[fp][0]

                        # single entry with the same fingerprint: this is DEFINITELY
                        # a move, and should appear in deleted_folders[]
                        assert other_path in deleted_folders, \
                            f"Moved folder '{other_path}' was not found in deleted_folders[]"

                        moved_folders.append((other_path, self_path))

                        prefixes_to_remove.append(other_path)
                        prefixes_to_remove.append(self_path)

                    # 2. at least one endpoint has multiple entries, we will
                    #    attempt to match by the top-level folder name.  this
                    #    is admittedly weak, but same folder name with the same
                    #    fingerprint is about the best we will get.  if this
                    #    fails, then we fall back to the default delete/add
                    #    action with data transfer
                    else:
                        fall_back = False
                        source_name_counts: Dict[str, List[int, int]] = {}
                        for index, source_path in enumerate(self_fingerprints[fp]):
                            name = os.path.split(source_path)[1]
                            if name not in source_name_counts:
                                source_name_counts[name] = [0, index]
                            source_name_counts[name][0] += 1

                            # if a fingerprint has more than one path with the same
                            # root folder name, we have to bail to the default action
                            if source_name_counts[name][0] > 1:
                                fall_back = True
                                break

                        if not fall_back:
                            other_name_counts: Dict[str, List[int, int]] = {}
                            for index, other_path in enumerate(other_fingerprints[fp]):
                                name = os.path.split(other_path)[1]
                                if name not in other_name_counts:
                                    other_name_counts[name] = [0, index]
                                other_name_counts[name][0] += 1

                                if other_name_counts[name][0] > 1:
                                    fall_back = True
                                    break

                        if not fall_back:
                            # no duplicate root folder names were found
                            for key in source_name_counts:
                                if key in other_name_counts:
                                    # assume this is it
                                    self_path = self_fingerprints[fp][source_name_counts[key][1]]
                                    other_path = other_fingerprints[fp][other_name_counts[key][1]]

                                    # make sure we check our assumptions
                                    assert other_path in deleted_folders, \
                                        f"Moved folder '{other_path}' was not found in deleted_folders[]"

                                    moved_folders.append((other_path, self_path))

                                    prefixes_to_remove.append(other_path)
                                    prefixes_to_remove.append(self_path)

                                    break

        if len(prefixes_to_remove):
            # we sort the prefixes to make sure they stream from longest to
            # shortest to help ensure unique matches
            t_prefixes_to_remove = tuple(sorted(prefixes_to_remove))
            added_files = [file for file in added_files if not file[0].startswith(t_prefixes_to_remove)]
            deleted_files = [file for file in deleted_files if not file[0].startswith(t_prefixes_to_remove)]
            added_folders = [folder for folder in added_folders if folder not in t_prefixes_to_remove]
            deleted_folders = [folder for folder in deleted_folders if folder not in t_prefixes_to_remove]

        # now process any remaining individual files

        moved_files = []
        prefixes_to_remove = []

        for dpath, dfile in deleted_files:
            for apath, afile in added_files:
                if afile == dfile:  # compare fingerprints
                    # highly likely that this is a file that has been moved
                    moved_files.append((dpath, apath))

                    prefixes_to_remove.append(dpath)
                    prefixes_to_remove.append(apath)

        if len(prefixes_to_remove):
            t_prefixes_to_remove = tuple(sorted(prefixes_to_remove))
            added_files = [file for file in added_files if not file[0].startswith(t_prefixes_to_remove)]
            deleted_files = [file for file in deleted_files if not file[0].startswith(t_prefixes_to_remove)]

        # run the calculated diff through the inxlude/exclude filters, if any
        if 'includes' in self._settings:
            def filter_includes(includes, paths):
                removals = []
                for path in paths:
                    for filter in includes:
                        if isinstance(filter, str):
                            if isinstance(path, str):
                                if filter not in path:
                                    removals.append(path)
                            elif isinstance(path, tuple):
                                if filter not in path[0]:
                                    removals.append(path)
                        else:   # regular expression
                            result = None
                            if isinstance(path, str):
                                result = filter.search(path)
                            elif isinstance(path, tuple):
                                result = filter.search(path[0])
                            if result is None:  # if it wasn't matched, exclude it
                                removals.append(path)

                return [path for path in paths if path not in removals]

            deleted_folders = filter_includes(self._settings.includes, deleted_folders) if len(deleted_folders) else []
            added_folders = filter_includes(self._settings.includes, added_folders) if len(added_folders) else []
            moved_folders = filter_includes(self._settings.includes, moved_folders) if len(moved_folders) else []
            deleted_files = filter_includes(self._settings.includes, deleted_files) if len(deleted_files) else []
            added_files = filter_includes(self._settings.includes, added_files) if len(added_files) else []
            moved_files = filter_includes(self._settings.includes, moved_files) if len(moved_files) else []
            modified_files = filter_includes(self._settings.includes, modified_files) if len(modified_files) else []
            resumeable_files = filter_includes(self._settings.includes, resumeable_files) if len(resumeable_files) else []

        elif 'excludes' in self._settings:
            def filter_excludes(excludes, paths):
                removals = []
                for path in paths:
                    for filter in excludes:
                        if isinstance(filter, str):
                            if isinstance(path, str):
                                if filter in path:
                                    removals.append(path)
                            elif isinstance(path, tuple):
                                if filter in path[0]:
                                    removals.append(path)
                        else:   # regular expression
                            result = None
                            if isinstance(path, str):
                                result = filter.search(path)
                            elif isinstance(path, tuple):
                                result = filter.search(path[0])
                            if result:  # if it was matched, exclude it
                                removals.append(path)

                return [path for path in paths if path not in removals]

            deleted_folders = filter_excludes(self._settings.excludes, deleted_folders) if len(deleted_folders) else []
            added_folders = filter_excludes(self._settings.excludes, added_folders) if len(added_folders) else []
            moved_folders = filter_excludes(self._settings.excludes, moved_folders) if len(moved_folders) else []
            deleted_files = filter_excludes(self._settings.excludes, deleted_files) if len(deleted_files) else []
            added_files = filter_excludes(self._settings.excludes, added_files) if len(added_files) else []
            moved_files = filter_excludes(self._settings.excludes, moved_files) if len(moved_files) else []
            modified_files = filter_excludes(self._settings.excludes, modified_files) if len(modified_files) else []
            resumeable_files = filter_excludes(self._settings.excludes, resumeable_files) if len(resumeable_files) else []

        print(f"Deleted folders: {deleted_folders}")
        print(f"Added folders: {added_folders}")
        print(f"Moved folders: {moved_folders}")
        print(f"Deleted files: {deleted_files}")
        print(f"Added files: {added_files}")
        print(f"Moved files: {moved_files}")
        print(f"Modified files: {modified_files}")
        print(f"Resumeable files: {resumeable_files}")
        sys.exit(0)

        folders_tuple = Types.DiffFolderTuple(deleted=deleted_folders,
                                              added=added_folders,
                                              moved=moved_folders)
        files_tuple = Types.DiffFilesTuple(deleted=deleted_files,
                                           added=added_files,
                                           moved=moved_files,
                                           modified=modified_files,
                                           resume=resumeable_files)

        return Types.DiffTuple(files=files_tuple, folders=folders_tuple)

    def acquire_stream(self, data: bytes) -> FileStream:
        """
        Create and return a FileStream instance that will signal if
        the file to which the stream belongs has an invalidating event
        """
        assert data not in self._active_streams, f"Provided stream id '{data}' is already active"

        fs = FileSystem.FileStream(self, data)
        if fs.is_file:
            self._active_streams[fs.id] = signaling.Signal(args=['event'])
            self._active_streams[fs.id].connect(fs._on_invalidation_event)

        return fs

    def release_stream(self, fs: FileStream) -> None:
        """ Clean up a FileStream instance """
        if fs.is_file:
            assert fs.id in self._active_streams, f"Provided stream id {fs.id} is not currently active"

            self._active_streams[fs.id].disconnect(fs._on_invalidation_event)
            del self._active_streams[fs.id]

    @property
    def folder_count(self) -> int:
        return self._counts.folders

    @property
    def file_count(self) -> int:
        return self._counts.files
