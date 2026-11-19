"""Read-only Markdown navigation, one bounded directory or document per request."""
import hashlib
import os
from pathlib import Path

from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from .io import no_symlinks, relative_path

DIRECTORY_ENTRIES = 4096
DIRECTORY_BYTES = 512 * 1024
DOCUMENT_BYTES = 1024 * 1024
SKIP = {'node_modules', '__pycache__', 'venv', 'env'}


def _path(root, value, *, directory=False):
    if directory and value == '':
        return no_symlinks(Path(root).absolute())
    if not isinstance(value, str) or len(value.encode('utf-8')) > 1024 or any(ord(c) < 32 for c in value):
        raise ForgeError('Invalid document path', code='invalid_request')
    parts = value.split('/')
    if (len(parts) > 24 or any(part.startswith('.') or part in SKIP or
                              part.endswith('.lance') for part in parts)):
        raise ForgeError('Document path is outside the visible project directory', code='invalid_request')
    return relative_path(root, value)


def listing(root, path='', offset=0, limit=50):
    if type(offset) is not int or not 0 <= offset <= DIRECTORY_ENTRIES or type(limit) is not int or not 1 <= limit <= 50:
        raise ForgeError('Invalid document page', code='invalid_request')
    directory = _path(root, path, directory=True)
    rows, size = [], 0
    try:
        with os.scandir(directory) as entries:
            for index, entry in enumerate(entries):
                size += len(entry.name.encode('utf-8'))
                if index >= DIRECTORY_ENTRIES or size > DIRECTORY_BYTES:
                    raise ForgeError('Document directory exceeds listing budget', code='resource_limit')
                if entry.name.startswith('.') or entry.name in SKIP or entry.name.endswith('.lance') or entry.is_symlink():
                    continue
                item = '/'.join(filter(None, [path, entry.name]))
                if len(item.encode('utf-8')) > 1024 or len(item.split('/')) > 24 or '\\' in item:
                    continue
                kind = ('directory' if entry.is_dir(follow_symlinks=False) else
                        'markdown' if Path(entry.name).suffix.lower() in {'.md', '.markdown'} and entry.is_file(follow_symlinks=False) else None)
                if kind:
                    rows.append(dict(name=entry.name, path=item, kind=kind))
        rows.sort(key=lambda row: (row['kind'] != 'directory', row['name'].casefold(), row['name']))
        return dict(path=path, entries=rows[offset:offset+limit],
                    next_offset=offset+limit if len(rows) > offset+limit else None)
    except FileNotFoundError as exc:
        raise ForgeError('Document directory no longer exists', code='not_found') from exc
    except OSError as exc:
        raise ForgeError('Document directory cannot be read', code='invalid_request') from exc


def read(root, path):
    target = _path(root, path)
    if target.suffix.lower() not in {'.md', '.markdown'}:
        raise ForgeError('Only Markdown documents can be opened here', code='invalid_request')
    try:
        raw = read_bounded(target, DOCUMENT_BYTES)
        return dict(path=path, sha256=hashlib.sha256(raw).hexdigest(), text=raw.decode('utf-8'))
    except FileNotFoundError as exc:
        raise ForgeError('Document no longer exists', code='not_found') from exc
    except (OSError, UnicodeError) as exc:
        raise ForgeError('Document must be a readable UTF-8 file', code='invalid_request') from exc
