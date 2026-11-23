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

from demiflow.execution.artifacts import resolve_local_artifact
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity, RecoveryContractError

_PREFIX = 'private-sqlite-'
_MINIMUM = 4 * 1024**2
_MAX_HISTORICAL_LEASES = 100000


def _lease_history(path, ceiling):
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


def _existing_size(path, ceiling):
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


def bind_private_storage(service):
    """Called once after both sealed model binders, before worker launch."""
    configurations = [*getattr(service, '_private_embedding_nodes', {}).values(),
                      *getattr(service, '_private_prompt_nodes', {}).values()]
    if not configurations:
        return
    paths = sorted({resolve_local_artifact(item['private_journal']['path']) for item in configurations})
    ceiling = service._bounds.max_spill_bytes
    from demiflow.operator_llm.sqlite_journal import MAX_DISK_BYTES
    journal = service._journal
    # The original reservation table is the authority for both existing path
    # caps and aggregate usage. Selection + reservation is one original fence
    # transaction, so concurrent prepares cannot over-assign the private half.
    with journal.execution_guard(service._fence) as db:
        used = db.execute("SELECT coalesce(sum(amount),0) FROM runtime_reservations "
                          "WHERE name='artifact_staging_bytes' AND reservation_id LIKE ?", (_PREFIX+'%',)).fetchone()[0]
        caps, fresh, leases = {}, [], {}
        for path in paths:
            reservation = _PREFIX + identity(str(path))
            row = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' "
                             'AND reservation_id=?', (reservation,)).fetchone()
            if row is None:
                fresh.append(path)
            else:
                caps[path] = row[0]
        if fresh:
            capacity = min(MAX_DISK_BYTES, (ceiling // 2 - used) // len(fresh))
            if capacity < _MINIMUM:
                raise DataflowError('BUDGET_EXCEEDED', 'Private SQLite paths exceed their shared 4 MiB per-path minimum')
            caps.update((path, capacity) for path in fresh)
        for path, capacity in caps.items():
            if not _MINIMUM <= capacity <= min(MAX_DISK_BYTES, ceiling // 2):
                raise RecoveryContractError('Original private SQLite storage capacity is incompatible')
            leases[path] = _lease_history(path, capacity)
            _existing_size(path, capacity)
            journal.reserve_budgets_in_transaction(db, _PREFIX + identity(str(path)),
                {'artifact_staging_bytes': (capacity, ceiling)})
    fact = {'schema': 'demiflow.private_model_storage.v1', 'paths': [
        {'path': str(path), 'max_disk_bytes': caps[path], 'classic_lease_files': {
            **leases[path], 'max_new_files': service._action.context.limits['max_requests'],
            'filesystem_metadata_byte_hard_limit': False}} for path in paths]}
    journal.bind_source(service._fence, service._action.action_id+'-private-model-storage', fact)
    service._private_storage_caps = {str(path): capacity for path, capacity in caps.items()}


def private_storage(service, declared_path):
    """Resolve only an already sealed path; no worker path is accepted."""
    path = resolve_local_artifact(declared_path)
    capacity = getattr(service, '_private_storage_caps', {}).get(str(path))
    if capacity is None:
        raise DataflowError('INVALID_CONTRACT', 'Private SQLite has no original storage reservation')
    _existing_size(path, capacity)
    return path, capacity
