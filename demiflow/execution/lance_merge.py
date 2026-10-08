"""Automatic isolated column-update preparation for exact Lance scans."""
from dataclasses import replace
import sys
import tempfile

import pyarrow as pa

from ..lance.mutate import _open, _merge_write_mode, _commit_merge
from ..lance.storage import open_lance_dataset
from .native_resources import dataset_session


def try_column_merge(executor, spec, query, source_rows):
    if (not sys.platform.startswith('linux') or source_rows is None
            or spec.mode != 'merge' or spec.when_not_matched != 'error'):
        return None
    target = _open(spec)
    source = open_lance_dataset(query.uri, query.version, query.storage_options)
    schema = spec.schema if spec.schema is not None else pa.schema(
        [source.schema.field(name) for name in (query.columns or source.schema.names)])
    if _merge_write_mode(spec, target.schema, schema, source_rows, target.count_rows()) != 'rewrite_columns':
        return None
    # Bind preparation to the same target version used for physical planning.
    # The worker stages files; only this original writer commits after success.
    fixed = replace(spec, expected_version=target.version)
    temp = executor._local_kernel.temp_directory if executor._local_kernel else None
    with tempfile.TemporaryDirectory(prefix='demiflow-merge-', dir=temp) as directory:
        with dataset_session(executor, directory, lance_merge=True) as session:
            prepared = session._prepare_lance_merge(fixed, query, source_rows)
            if prepared.read_version != target.version:
                raise RuntimeError('Prepared merge returned a different target version')
            return _commit_merge(spec, prepared)
