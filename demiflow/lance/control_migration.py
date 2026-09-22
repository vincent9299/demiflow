"""Explicit relocation of idle local control files; never a runtime fallback.

Requires a maintenance window with old writers stopped. Both old and new lock
inodes are held throughout the move; active or unfinished transactions abort.
"""
from contextlib import ExitStack
from pathlib import Path
import fcntl
import os
from .control import control_directory, table_lock_path


def migrate_local_controls(root):
    root = Path(root)
    selected = {}
    for parent, directories, files in os.walk(root):
        directories[:] = [d for d in directories if not d.endswith('.lance') and d not in {'_demiflow', '_staging'}]
        directory = Path(parent)
        for name in files:
            path = directory / name
            suffix = next((s for s in ('.demiflow-checkpoint.json', '.demiflow-checkpoint.lock', '.lance.lock') if name.endswith(s)), None)
            if suffix:
                table = directory / (name[:-len(suffix)] + ('.lance' if suffix == '.lance.lock' else ''))
            elif name.endswith('.lock') and (directory / (name[:-5] + '.lance')).is_dir():
                table = directory / (name[:-5] + '.lance')
            else:
                continue
            if not table.is_dir():
                # Orphans require an explicit retention decision, not guessing.
                raise ValueError('Control file without a live table: ' + str(path))
            selected.setdefault(table, []).append(path)
    result = []
    for table, paths in sorted(selected.items()):
        parent = table.parent
        if list(parent.glob(table.name + '.pending-*.json')):
            raise ValueError('Unrecovered transaction: ' + str(table))
        with ExitStack() as stack:
            locks = [p for p in paths if p.suffix == '.lock']
            for path in [table_lock_path(table), *locks]:
                lock = stack.enter_context(path.open('a'))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            receipts = [p for p in paths if p.suffix == '.json']
            for old in receipts:
                target = control_directory(table) / 'checkpoint.json'
                if target.exists() and old.read_bytes() != target.read_bytes():
                    raise ValueError('Conflicting control receipts: ' + str(old))
                os.replace(old, target)
                result.append({'from': str(old.relative_to(root)), 'to': str(target.relative_to(root))})
            for old in locks:
                old.unlink()
                result.append({'from': str(old.relative_to(root)), 'to': str(table_lock_path(table).relative_to(root))})
    return result
