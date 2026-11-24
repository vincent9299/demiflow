"""Bounded local write accounting without scanning a table's historical files.

Linux notifications cover declared output roots before the worker starts. Only
changed files are measured. Lost notifications fail closed. A bounded recent
file cache can overcount an evicted file when it changes again; it never imposes
a lifetime file-count limit. Concurrent external writes may overcount; this is not a quota or a
transaction lock. Native batch admission remains the before-allocation guard.
"""
import ctypes
from collections import OrderedDict
import os
from pathlib import Path
import stat
import struct

_EVENT = struct.Struct('iIII')
_DIRECTORY = 0x40000000
_OVERFLOW = 0x4000
_MASK = 0x2 | 0x4 | 0x8 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800


class OutputWatch:
    def __init__(self, roots, *, max_files=8192):
        self.roots = tuple(Path(root).absolute() for root in roots)
        if not 1 <= len(self.roots) <= 32 or type(max_files) is not int or not 1 <= max_files <= 8192:
            raise ValueError('Invalid output watch budget')
        self.max_files = max_files
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.inotify_init1.argtypes = [ctypes.c_int]
        self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), 'Cannot establish output accounting')
        self.watches, self.identities, self.paths = {}, OrderedDict(), {}
        self.total = 0
        try:
            for root in self.roots:
                self._watch(root.parent)
                if root.exists():
                    self._watch(root)
                    for name in ('data', '_versions', '_transactions', '_deletions'):
                        path = root / name
                        if path.exists():
                            self._watch(path)
        except BaseException:
            self.close()
            raise

    def _watch(self, path):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('Output watch requires real directories')
        if len(self.watches) >= 512:
            raise ValueError('Output directory watch budget exceeded')
        descriptor = self.libc.inotify_add_watch(self.fd, os.fsencode(path), _MASK | 0x02000000 | 0x01000000)
        if descriptor < 0:
            raise OSError(ctypes.get_errno(), 'Cannot watch declared output directory')
        self.watches[descriptor] = path

    def _measure(self, path, *, created=False):
        try:
            info = path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Output allocation must be a regular file')
        identity = (info.st_dev, info.st_ino)
        replaced = self.paths.get(path)
        if replaced is not None and replaced != identity:
            self.identities.pop(replaced, None)
        previous = self.identities.pop(identity, (0, None))
        self.paths.pop(previous[1], None)
        # A new directory entry may reuse an inode that the filesystem freed.
        # Renames retain the high-water mark; creates conservatively start anew.
        old = 0 if created else previous[0]
        self.total += max(0, info.st_size - old)
        self.identities[identity] = (max(old, info.st_size), path)
        self.paths[path] = identity
        if len(self.identities) > self.max_files:
            _, (_, evicted_path) = self.identities.popitem(last=False)
            self.paths.pop(evicted_path, None)

    def _new_directory(self, path, depth=0):
        if depth > 8:
            raise ValueError('New output directory depth exceeded')
        self._watch(path)  # Subscribe before scanning the newly created directory.
        with os.scandir(path) as entries:
            for index, entry in enumerate(entries):
                if index >= 8192:
                    raise ValueError('New output directory exceeds entry budget')
                if entry.is_dir(follow_symlinks=False):
                    self._new_directory(Path(entry.path), depth + 1)
                else:
                    self._measure(Path(entry.path))

    def poll(self):
        events = 0
        while True:
            try:
                raw = os.read(self.fd, 65536)
            except BlockingIOError:
                return self.total
            if not raw:
                raise ValueError('Output accounting stream closed')
            offset = 0
            while offset < len(raw):
                descriptor, mask, _, length = _EVENT.unpack_from(raw, offset)
                offset += _EVENT.size
                name = os.fsdecode(raw[offset:offset + length].split(b'\0', 1)[0])
                offset += length
                events += 1
                if mask & _OVERFLOW or events > 32768:
                    raise ValueError('Output accounting notification budget lost')
                parent = self.watches.get(descriptor)
                if parent is None:
                    raise ValueError('Unknown output watch descriptor')
                if mask & (0x400 | 0x800 | 0x8000):
                    raise ValueError('Watched output directory moved or disappeared')
                path = parent / name
                if not any(path == root or path.is_relative_to(root) for root in self.roots):
                    continue
                if mask & _DIRECTORY:
                    if mask & (0x100 | 0x80):
                        self._new_directory(path)
                elif mask & 0x200:
                    # Drop deleted paths so a later reused inode cannot inherit
                    # their allocation. Both indexes have the same bounded size.
                    identity = self.paths.pop(path, None)
                    if identity is not None:
                        self.identities.pop(identity, None)
                else:
                    self._measure(path, created=bool(mask & 0x100))

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1
