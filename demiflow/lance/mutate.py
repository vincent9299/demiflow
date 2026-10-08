"""Keyed partial updates and explicit schema additions; no business field policies."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import logging
import pickle
import sqlite3
import tempfile
from pathlib import Path

import pyarrow as pa

from demiflow._compat.error_transport import error_from_exception
from ..errors import InvalidLanceRequest, LanceWriteConflict, LanceWriteError
from .model import LanceWriteReceipt, LanceWriteSpec
from .storage import require_lance, schema_hash, lance_commit_conflict_error

_LOG = logging.getLogger(__name__)
_KEY_MEMORY_ROWS = 100_000


class _UniqueMergeKeys:
    """Exact duplicate detection with a bounded in-memory prefix and disk B-tree.

    Store full typed keys, not hashes. This database is disposable validation
    state: journaling/durability are unnecessary before the Lance commit.
    """
    def __init__(self):
        self.seen = set()
        self.directory = None
        self.connection = None

    def check(self, table, columns):
        for batch in table.select(columns).to_batches(max_chunksize=8192):
            self._check_batch(batch, columns)

    def _check_batch(self, batch, columns):
        keys = [tuple(row[k] for k in columns) for row in batch.to_pylist()]
        if any(any(value is None for value in key) for key in keys):
            raise InvalidLanceRequest('Patch merge keys must be unique and non-null')
        if self.connection is None and len(self.seen) + len(keys) > _KEY_MEMORY_ROWS:
            self.directory = tempfile.TemporaryDirectory(prefix='demiflow-merge-keys-')
            # Arrow may pull batches on a native reader thread; access is serial.
            self.connection = sqlite3.connect(str(Path(self.directory.name) / 'keys.sqlite'), check_same_thread=False)
            self.connection.execute('PRAGMA journal_mode=OFF')
            self.connection.execute('PRAGMA synchronous=OFF')
            self.connection.execute('PRAGMA cache_size=-8192')
            self.connection.execute('PRAGMA mmap_size=0')
            self.connection.execute('CREATE TABLE keys (value BLOB PRIMARY KEY) WITHOUT ROWID')
            self.connection.executemany('INSERT INTO keys VALUES (?)',
                ((pickle.dumps(key, protocol=5),) for key in self.seen))
            self.seen.clear()
        if self.connection is not None:
            try:
                self.connection.executemany('INSERT INTO keys VALUES (?)',
                    ((pickle.dumps(key, protocol=5),) for key in keys))
            except sqlite3.IntegrityError as error:
                raise InvalidLanceRequest('Patch merge keys must be unique and non-null') from error
        else:
            for key in keys:
                if key in self.seen:
                    raise InvalidLanceRequest('Patch merge keys must be unique and non-null')
                self.seen.add(key)

    def close(self):
        if self.connection is not None:
            self.connection.close()
        if self.directory is not None:
            self.directory.cleanup()
        self.seen.clear()


def _has_special_storage(field):
    """Column rewrites cannot carry blobs; unknown extensions stay native-auto."""
    metadata = field.metadata or {}
    if (metadata.get(b'lance-encoding:blob') == b'true'
            or b'ARROW:extension:name' in metadata
            or isinstance(field.type, pa.BaseExtensionType)):
        return True
    return any(_has_special_storage(field.type.field(i))
               for i in range(field.type.num_fields))


def _merge_write_mode(spec, target_schema, patch_schema, source_rows, target_rows):
    """Keep omitted payload out of known, update-only partial patches.

    Row density alone cannot bound the cost of reading untouched wide columns.
    Sparse patches also use column files, accepting fragment-level write
    amplification in exchange for avoiding that payload. Unknown/filtered
    streams retain native auto; key validation still precedes commit.
    """
    if (spec.when_not_matched != 'error' or source_rows is None or not target_rows
            or not 0 < source_rows <= target_rows
            or not set(spec.on) < set(patch_schema.names) < set(target_schema.names)):
        return 'auto'
    if any(_has_special_storage(field) or _has_special_storage(target_schema.field(field.name))
           for field in patch_schema):
        return 'auto'
    return 'rewrite_columns'


class _VersionGuard:
    """Check the actual manifest commit, including native automatic rebases."""
    def __init__(self, version):
        self.version = version
        self.conflict = None

    @contextmanager
    def __call__(self, version):
        if version != self.version + 1:
            self.conflict = LanceWriteConflict(
                f'Lance mutation expected version {self.version}, attempted commit {version}')
            raise self.conflict
        yield


def _open(spec, *, commit_lock=None):
    ds = require_lance().dataset(spec.uri, storage_options=dict(spec.storage_options) or None,
                                commit_lock=commit_lock)
    if spec.expected_version is not None and ds.version != spec.expected_version:
        raise LanceWriteConflict(f'Lance mutation expected version {spec.expected_version}, current is {ds.version}')
    return ds


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _target_keys(ds, keys):
    for key in keys:
        if key not in ds.schema.names:
            raise InvalidLanceRequest(f'Merge key absent from target: {key}')
        field = ds.schema.field(key)
        if not (pa.types.is_integer(field.type) or pa.types.is_string(field.type)
                or pa.types.is_large_string(field.type) or pa.types.is_binary(field.type)):
            raise InvalidLanceRequest('Merge keys must be integer, string or binary columns')
    if ds.count_rows(filter=' OR '.join(_quote(k) + ' IS NULL' for k in keys)):
        raise InvalidLanceRequest('Target merge keys contain nulls')
    names = ', '.join(_quote(k) for k in keys)
    duplicate = ds.sql(f'SELECT {names} FROM dataset GROUP BY {names} HAVING count(*) > 1 LIMIT 1')
    if any(batch.num_rows for batch in duplicate.build().to_batch_records()):
        raise InvalidLanceRequest('Target merge keys are not unique')


def _type_nulls(array, target):
    """Give inferred null leaves their target type before Arrow's safe cast.

    Arrow can mis-size null children while casting nested list<struct> arrays.
    Preserve their lengths explicitly; non-null values still use checked casts.
    """
    if pa.types.is_null(array.type):
        return pa.nulls(len(array), type=target)
    if pa.types.is_struct(array.type) and pa.types.is_struct(target):
        fields = {f.name: f.type for f in target}
        children = [_type_nulls(array.field(f.name), fields.get(f.name, f.type)) for f in array.type]
        return pa.StructArray.from_arrays(children, names=[f.name for f in array.type], mask=array.is_null())
    if (pa.types.is_list(array.type) or pa.types.is_large_list(array.type)) and (
            pa.types.is_list(target) or pa.types.is_large_list(target)):
        cls = pa.LargeListArray if pa.types.is_large_list(array.type) else pa.ListArray
        return cls.from_arrays(array.offsets, _type_nulls(array.values, target.value_type), mask=array.is_null())
    return array


def _cast_patch(table, schema):
    if len(table.column_names) != len(schema.names) or set(table.column_names) != set(schema.names):
        raise InvalidLanceRequest('Every patch batch must contain exactly the declared columns')
    if table.schema == schema:
        return table
    columns = [pa.chunked_array([_type_nulls(chunk, field.type).cast(field.type, safe=True)
                                for chunk in table[field.name].chunks], type=field.type)
               for field in schema]
    return pa.Table.from_arrays(columns, schema=schema)


def merge_lance(spec, batches):
    return _commit_merge(spec, _prepare_merge(spec, batches))


@dataclass(frozen=True)
class _PreparedMerge:
    transaction: object
    read_version: int
    schema_hash: str
    input_rows: int
    updated_rows: int
    inserted_rows: int
    ignored_rows: int


def _prepare_merge(spec, batches, *, _source_row_count=None):
    """Stream and validate one patch into an uncommitted native transaction.

    Source key validation spills after a bounded prefix; payload batches stay streamed.
    Missing keys/invalid patches fail before commit. Native staging files may remain
    after a failed validation, but no target version is published by that failure.
    """
    from .write import _normalize_tables
    lance = require_lance()
    ds = _open(spec)
    _target_keys(ds, spec.on)
    tables = iter(_normalize_tables(batches))
    first = next(tables, None)
    names = (spec.schema.names if spec.schema is not None else first.schema.names if first is not None
             else [*spec.on, *(spec.update_columns or ())])
    columns = set(names)
    if not set(spec.on) <= columns or not columns <= set(ds.schema.names):
        raise InvalidLanceRequest('Patch requires all merge keys and only existing target columns')
    if spec.update_columns is not None and columns != set(spec.on) | set(spec.update_columns):
        raise InvalidLanceRequest('Patch columns differ from the declared update_columns')
    # Dataset row execution may infer int64/string/list<null>; align values to
    # existing target types using Arrow's checked cast, never evolve the schema.
    patch_schema = spec.schema if spec.schema is not None else pa.schema([ds.schema.field(k) for k in names])
    for field in patch_schema:
        if field.type != ds.schema.field(field.name).type:
            raise InvalidLanceRequest(f'Patch column type differs from target: {field.name}')
    seen, validation = _UniqueMergeKeys(), []
    count = 0

    def checked():
        nonlocal count
        from itertools import chain
        for table in chain(() if first is None else (first,), tables):
            try:
                table = _cast_patch(table, patch_schema)
                seen.check(table, spec.on)
                count += table.num_rows
            except Exception as exc:
                validation.append(exc)
                raise
            yield from table.to_batches()

    builder = ds.merge_insert(list(spec.on)).when_matched_update_all()
    target_rows = ds.count_rows()
    write_mode = _merge_write_mode(spec, ds.schema, patch_schema,
                                  _source_row_count, target_rows)
    _LOG.info('Lance merge write_mode=%s source_rows=%s target_rows=%s',
              write_mode, _source_row_count, target_rows)
    if write_mode != 'auto':
        builder = builder.write_mode(write_mode)
    if spec.when_not_matched == 'insert':
        builder = builder.when_not_matched_insert_all()
    try:
        transaction, stats = builder.execute_uncommitted(pa.RecordBatchReader.from_batches(patch_schema, checked()))
    except Exception:
        if validation:
            raise validation[0]
        raise
    finally:
        seen.close()
    updated, inserted = stats['num_updated_rows'], stats['num_inserted_rows']
    ignored = count - updated - inserted
    if ignored < 0:
        raise InvalidLanceRequest('Merge matched more target rows than source keys')
    if ignored and spec.when_not_matched == 'error':
        raise InvalidLanceRequest(f'{ignored} patch keys are absent from the target')
    return _PreparedMerge(transaction, ds.version, schema_hash(ds.schema), count, updated, inserted, ignored)


def _commit_merge(spec, prepared):
    """Only the original writer commits, including results from isolated workers."""
    lance = require_lance()
    guard = _VersionGuard(prepared.read_version)
    updated, inserted, count = prepared.updated_rows, prepared.inserted_rows, prepared.input_rows
    counts = dict(updated_rows=updated, inserted_rows=inserted, ignored_rows=prepared.ignored_rows)
    version = prepared.read_version
    if updated or inserted:
        try:
            committed = lance.LanceDataset.commit(
                spec.uri, prepared.transaction, commit_lock=guard, max_retries=0,
                storage_options=dict(spec.storage_options) or None)
            version = committed.version
        except Exception as exc:
            if guard.conflict:
                raise guard.conflict from exc
            if isinstance(exc, lance_commit_conflict_error()):
                raise LanceWriteConflict(str(exc)) from exc
            return LanceWriteReceipt(spec.content_hash, spec.uri, spec.expected_version,
                None, count, None, prepared.schema_hash, 'indeterminate', error_from_exception(exc))
    elif _open(spec).version != prepared.read_version:
        raise LanceWriteConflict('Lance target changed during a no-op merge')
    return LanceWriteReceipt(spec.content_hash, spec.uri, spec.expected_version,
        version, count, updated + inserted, prepared.schema_hash, 'committed', merge_stats=counts)


def add_lance_columns(uri, columns, *, expected_version=None, storage_options=None):
    """Add explicitly declared nullable top-level fields, initially null.

    This is a metadata-only native schema operation. Existing fields cannot be
    replaced or silently widened. Populate the new fields with a subsequent merge.
    Return the actual committed version; conflicts and uncertain commits are explicit.
    """
    spec = LanceWriteSpec(uri, expected_version=expected_version, storage_options=storage_options)
    if not isinstance(columns, pa.Schema) or not len(columns):
        raise InvalidLanceRequest('add_lance_columns requires a nonempty Arrow schema')
    if len(set(columns.names)) != len(columns.names) or any(not f.nullable for f in columns):
        raise InvalidLanceRequest('New column names must be unique and all fields nullable')
    original = _open(spec)
    if set(columns.names) & set(original.schema.names):
        raise InvalidLanceRequest('add_lance_columns cannot replace existing columns')
    guard = _VersionGuard(original.version)
    ds = require_lance().dataset(spec.uri, version=original.version, commit_lock=guard,
                                storage_options=dict(spec.storage_options) or None)
    identity = hashlib.sha256(json.dumps({**spec.to_dict(), 'kind': 'add_columns',
        'columns': schema_hash(columns)}, sort_keys=True).encode()).hexdigest()
    try:
        ds.add_columns(columns)
    except Exception as exc:
        if guard.conflict:
            raise guard.conflict from exc
        if isinstance(exc, lance_commit_conflict_error()):
            raise LanceWriteConflict(str(exc)) from exc
        raise LanceWriteError(LanceWriteReceipt(identity, spec.uri, expected_version, None,
            0, None, schema_hash(original.schema), 'indeterminate', error_from_exception(exc))) from exc
    return LanceWriteReceipt(identity, spec.uri, expected_version, ds.version,
                             0, 0, schema_hash(ds.schema), 'committed')
