"""Private model SQLite storage charged to the original run staging budget.

Half of the run's spill ceiling is an internal upper bound for all private
paths; the other half remains available to immutable artifacts and exchanges.
Persistent caches retain their reservation for the run lifetime. This module
owns no allocator, file deletion, HTTP state or independent accounting table.
"""
from pathlib import Path
import os
import sqlite3
import stat


from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import RecoveryContractError


_MAX_HISTORICAL_LEASES = 100000


def inspect_private_lease_history(path, ceiling):
    """Bounded metadata inspection; never remove old lease files or locks.

    New files are empty and their count is additionally bounded by the original
    run request ceiling. Inode/directory allocation is filesystem dependent,
    so this count is not presented as a portable metadata byte hard limit.
    """
    directory = Path(str(path)+'.leases')
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        return {'existing_files': 0, 'existing_file_bytes': 0}
    count = size = 0
    with entries:
        for entry in entries:
            count += 1
            if count > _MAX_HISTORICAL_LEASES:
                raise DataflowError('BUDGET_EXCEEDED', 'Private SQLite historical lease count exceeds bounded inspection')
            info = entry.stat(follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or len(entry.name) != 64
                    or any(c not in '0123456789abcdef' for c in entry.name)):
                raise RecoveryContractError('Private SQLite lease history contains an unsupported entry')
            if info.st_size:
                raise RecoveryContractError('Original private SQLite lease files must be empty; history is not rewritten')
            size += info.st_size
            if size > ceiling:
                raise DataflowError('BUDGET_EXCEEDED', 'Private SQLite lease file bytes exceed storage reservation')
    return {'existing_files': count, 'existing_file_bytes': size}


def inspect_private_journal(path, ceiling):
    """Reject incompatible history before a writable connection can touch it."""
    total = 0
    for suffix in ('', '-journal', '-wal', '-shm'):
        file = Path(str(path) + suffix)
        try:
            info = file.stat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode):
            raise RecoveryContractError('Private SQLite history must be regular files')
        total += info.st_size
    if total > ceiling:
        raise DataflowError('BUDGET_EXCEEDED', 'Existing private SQLite exceeds its original storage reservation')
    if not path.exists():
        return
    # A single DELETE-mode database is the existing bounded SQLite contract.
    # Never convert historical WAL mode or shrink a historical database here.
    with path.open('rb') as file:
        header = file.read(20)
    if not header:
        return
    if header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01':
        raise RecoveryContractError('Bounded private SQLite requires existing DELETE mode; history is not converted')
    # Keep SQLite's read locks: another declared route may already be writing
    # this same DELETE database. The header check above rejects WAL beforehand.
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)
    try:
        page_size = db.execute('PRAGMA page_size').fetchone()[0]
        pages = db.execute('PRAGMA page_count').fetchone()[0]
    finally:
        db.close()
    if pages > (ceiling - 65536) // (4 * page_size):
        raise DataflowError('BUDGET_EXCEEDED', 'Existing private SQLite pages exceed its original storage reservation')
