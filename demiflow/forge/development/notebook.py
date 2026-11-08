"""Read notebook code/tags with bounded memory, without retaining saved outputs."""
from __future__ import annotations

import json
import os
import re
import stat
import time

from .project import ForgeError, file_identity, no_symlinks

_SPECIAL = re.compile(r'["\\\x00-\x1f]')
_NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
_KEEP = {(), ('cells',), ('cells', '*'), ('cells', '*', 'cell_type'),
         ('cells', '*', 'source'), ('cells', '*', 'source', '*'),
         ('cells', '*', 'metadata'), ('cells', '*', 'metadata', 'tags'),
         ('cells', '*', 'metadata', 'tags', '*')}


class _Reader:
    def __init__(self, stream, maximum, code_bytes):
        self.stream, self.maximum = stream, maximum
        self.buffer, self.pos, self.total, self.kept = '', 0, 0, 0
        self.deadline = time.monotonic() + 30
        self.code_bytes = code_bytes

    def char(self):
        if time.monotonic() > self.deadline:
            raise ForgeError('Notebook parsing exceeded 30 seconds')
        if self.pos == len(self.buffer):
            self.buffer, self.pos = self.stream.read(65536), 0
            self.total += len(self.buffer.encode('utf-8'))
            if self.total > self.maximum:
                raise ForgeError('Notebook exceeds declared byte budget')
        return self.buffer[self.pos:self.pos + 1]

    def whitespace(self):
        while self.char() in (' ', '\t', '\n', '\r'):
            self.pos += 1

    def take(self, value):
        if self.char() != value:
            raise ForgeError('Invalid notebook JSON')
        self.pos += 1

    def string(self, keep):
        self.take('"')
        pieces = ['"'] if keep else None

        def append(text):
            if keep:
                self.kept += len(text.encode('utf-8'))
                if self.kept > self.code_bytes:
                    raise ForgeError('Notebook code/tags exceed declared projection budget')
                pieces.append(text)

        while self.char():
            match = _SPECIAL.search(self.buffer, self.pos)
            end = match.start() if match else len(self.buffer)
            append(self.buffer[self.pos:end])
            self.pos = end
            if match is None:
                continue
            char = self.char()
            self.pos += 1
            if char == '"':
                if keep:
                    pieces.append('"')
                    return json.loads(''.join(pieces))
                return None
            if char != '\\':
                raise ForgeError('Invalid control character in notebook JSON')
            escaped = self.char()
            if escaped not in ('"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u'):
                raise ForgeError('Invalid notebook JSON escape')
            self.pos += 1
            append('\\' + escaped)
            if escaped == 'u':
                for _ in range(4):
                    digit = self.char()
                    if not digit or digit not in '0123456789abcdefABCDEF':
                        raise ForgeError('Invalid notebook Unicode escape')
                    self.pos += 1
                    append(digit)
        raise ForgeError('Unterminated notebook JSON string')

    def value(self, path=(), depth=0):
        if depth > 64:
            raise ForgeError('Notebook JSON nesting exceeds 64')
        self.whitespace()
        keep = path in _KEEP
        char = self.char()
        if char == '"':
            return self.string(keep)
        if char in ('{', '['):
            object_value = char == '{'
            close = '}' if object_value else ']'
            self.pos += 1
            result = {} if object_value else []
            self.whitespace()
            if self.char() == close:
                self.pos += 1
                return result if keep else None
            count = 0
            while True:
                self.whitespace()
                key = self.string(keep) if object_value else '*'
                if object_value:
                    self.whitespace()
                    self.take(':')
                child_path = path + (key,)
                child = self.value(child_path, depth + 1)
                if child_path in _KEEP:
                    if object_value:
                        if key in result:
                            raise ForgeError('Duplicate structural notebook field')
                        result[key] = child
                    else:
                        result.append(child)
                count += 1
                if keep and count > 4096:
                    raise ForgeError('Notebook structure exceeds 4096 items per container')
                self.whitespace()
                if self.char() == close:
                    self.pos += 1
                    return result if keep else None
                self.take(',')
        token = ''
        while self.char() and self.char() not in ' \t\n\r,]}':
            token += self.char()
            self.pos += 1
            if len(token) > 1024:
                raise ForgeError('Notebook JSON scalar exceeds 1024 characters')
        if token not in ('true', 'false', 'null') and not _NUMBER.fullmatch(token):
            raise ForgeError('Invalid notebook JSON scalar')
        return json.loads(token) if keep else None


def read_cells(path, *, max_bytes=256 * 1024 * 1024, code_bytes=1024*1024):
    no_symlinks(path)
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes:
        raise ForgeError('Notebook is not a regular file within its byte budget')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'r', encoding='utf-8') as stream:
        if file_identity(os.fstat(stream.fileno())) != file_identity(before):
            raise ForgeError('Notebook changed before reading')
        reader = _Reader(stream, max_bytes, code_bytes)
        value = reader.value()
        reader.whitespace()
        if reader.char():
            raise ForgeError('Trailing notebook JSON content')
        if file_identity(os.fstat(stream.fileno())) != file_identity(before):
            raise ForgeError('Notebook changed while reading')
    no_symlinks(path)
    if file_identity(path.stat()) != file_identity(before):
        raise ForgeError('Notebook changed while reading')
    if type(value) is not dict or type(value.get('cells')) is not list:
        raise ForgeError('Notebook requires a cells array')
    for cell in value['cells']:
        if type(cell) is not dict or type(cell.get('cell_type')) is not str:
            raise ForgeError('Invalid notebook cell')
        source, metadata = cell.get('source'), cell.get('metadata', {})
        if not (isinstance(source, str) or isinstance(source, list) and all(isinstance(v, str) for v in source)):
            raise ForgeError('Invalid notebook source')
        if type(metadata) is not dict or type(metadata.get('tags', [])) is not list or any(not isinstance(t, str) for t in metadata.get('tags', [])):
            raise ForgeError('Invalid notebook tags')
    return value['cells']
