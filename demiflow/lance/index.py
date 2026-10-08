"""CPU-only, bounded Lance index staging followed by an optimistic manifest commit."""
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

from ..errors import LanceWriteConflict
from ..execution.datafusion import DataFusionOptions, DataFusionSession, _save
from .control import control_directory
from .storage import normalize_lance_uri, open_lance_dataset, require_lance, schema_hash


def vector_index_config(value=None):
    """Validate index parameters and finite CPU/RSS/disk/deadline budgets, without I/O."""
    defaults = dict(name='embedding_ivf_hnsw_sq', index_type='IVF_HNSW_SQ', metric='cosine',
        num_partitions=256, m=16, ef_construction=200, sample_rate=256, max_iterations=50,
        threads=8, memory_bytes=8 * 2**30, max_rss_bytes=20 * 2**30,
        max_scratch_bytes=32 * 2**30, max_index_bytes=16 * 2**30,
        timeout_s=3600, admission_timeout_s=3600, cgroup_headroom_bytes=2 * 2**30)
    if value is not None and (not isinstance(value, dict) or set(value) - set(defaults)):
        raise ValueError('Unknown Lance vector index configuration fields')
    cfg = {**defaults, **(value or {})}
    if not isinstance(cfg['name'], str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,79}', cfg['name']):
        raise ValueError('Index name must be a safe identifier of at most 80 characters')
    if cfg['index_type'] != 'IVF_HNSW_SQ' or cfg['metric'] != 'cosine':
        raise ValueError('This index builder supports IVF_HNSW_SQ with cosine')
    for key in set(defaults) - {'name', 'index_type', 'metric', 'timeout_s', 'admission_timeout_s'}:
        if type(cfg[key]) is not int or cfg[key] < 1:
            raise ValueError(key + ' must be a positive integer')
    if cfg['m'] < 4 or cfg['ef_construction'] < cfg['m']:
        raise ValueError('Require m >= 4 and ef_construction >= m')
    _resource_options(cfg)
    return cfg


def _resource_options(cfg):
    return DataFusionOptions(**{k: cfg[k] for k in (
        'threads', 'memory_bytes', 'max_rss_bytes', 'max_scratch_bytes',
        'timeout_s', 'admission_timeout_s', 'cgroup_headroom_bytes')}, partitions=1, batch_rows=256)


def _parameters(cfg, column):
    return {'column': column, **{k: cfg[k] for k in (
        'index_type', 'metric', 'num_partitions', 'm', 'ef_construction',
        'sample_rate', 'max_iterations')}}


def _record_path(uri, index_uuid):
    return control_directory(uri) / 'vector_indices' / (index_uuid + '.json')


def _statistics(ds, cfg):
    """Runs only in the isolated worker with centroids excluded from statistics."""
    stats = ds.stats.index_stats(cfg['name'])
    segments = stats.get('indices', [])
    matches = (stats['index_type'] == cfg['index_type'] and bool(segments)
        and all(s['metric_type'] == cfg['metric'] and s['num_partitions'] == cfg['num_partitions']
                and s['sub_index']['params']['m'] == cfg['m']
                and s['sub_index']['params']['ef_construction'] == cfg['ef_construction'] for s in segments))
    coverage = {k: int(stats[k]) for k in (
        'num_indexed_rows', 'num_unindexed_rows', 'num_indexed_fragments', 'num_unindexed_fragments')}
    return matches, coverage


def _prepare_index(request):
    """Inspect or write uncommitted files; never publish a manifest in this worker."""
    import numpy as np
    import pyarrow as pa
    from lance.dataset import Index
    cfg, uri, column = request['config'], request['uri'], request['column']
    pa.set_cpu_count(cfg['threads'])
    pa.set_io_thread_count(min(cfg['threads'], 4))
    ds = open_lance_dataset(uri, request['version'], index_cache_size_bytes=32 * 2**20,
                            metadata_cache_size_bytes=32 * 2**20)
    field = ds.schema.field(column)
    if not pa.types.is_fixed_size_list(field.type) or not pa.types.is_float32(field.type.value_type):
        raise ValueError('Index column must be a fixed-size list of float32')
    dimension, rows = field.type.list_size, ds.count_rows()
    parameters = _parameters(cfg, column)
    contract = {'parameters': parameters, 'schema_hash': schema_hash(ds.schema)}
    base = dict(rows=rows, parameters=parameters, schema_hash=contract['schema_hash'])
    if not rows:
        return {**base, 'action': 'empty', 'indices': [], 'coverage': {
            'num_indexed_rows': 0, 'num_unindexed_rows': 0,
            'num_indexed_fragments': 0, 'num_unindexed_fragments': 0}}
    existing = [i for i in ds.list_indices() if i['name'] == cfg['name']]
    if len(existing) > 64:
        raise ValueError('More than 64 index segments; explicit maintenance is required')
    if existing:
        matched, coverage = _statistics(ds, cfg)
        evidence = []
        for entry in existing:
            record = _record_path(uri, entry['uuid'])
            evidence.append(record.is_file() and record.stat().st_size <= 65536
                and json.loads(record.read_text()).get('contract') == contract)
        complete = (matched and all(i['fields'] == [column] for i in existing)
            and coverage['num_unindexed_rows'] == 0 and coverage['num_indexed_rows'] == rows
            and coverage['num_unindexed_fragments'] == 0)
        if request.get('inspect_only'):
            if not complete or [i['uuid'] for i in existing] != [request['uuid']]:
                raise RuntimeError('Committed index does not cover the expected snapshot or parameters')
        if complete and (all(evidence) or request.get('inspect_only')):
            return {**base, 'action': 'reuse', 'coverage': coverage,
                    'indices': [i['uuid'] for i in existing]}
    if request.get('inspect_only'):
        raise RuntimeError('Expected committed index is missing')
    if cfg['num_partitions'] > rows:
        raise ValueError('num_partitions exceeds row count; choose fewer partitions for this scope')
    # Preflight major training arrays, while retaining independent RSS and disk guards.
    training_bytes = min(rows, cfg['num_partitions'] * cfg['sample_rate']) * dimension * 4
    if training_bytes * 4 > cfg['memory_bytes']:
        raise ValueError('IVF training estimate exceeds memory_bytes; configure an adequate budget')
    if shutil.disk_usage(uri).free < cfg['max_index_bytes'] + cfg['max_scratch_bytes']:
        raise OSError('Insufficient disk headroom for declared index and scratch budgets')
    # Do not allow Lance to silently omit null/NaN/zero vectors. Only one bounded
    # numeric batch is materialized here; the dataset never becomes a Python list.
    for batch in ds.to_batches(columns=[column], batch_size=256, batch_readahead=1, fragment_readahead=1):
        values = batch.column(0)
        flat = values.flatten()
        if values.null_count or flat.null_count:
            raise ValueError('Index input contains null vectors or components')
        matrix = flat.to_numpy(zero_copy_only=False).reshape(-1, dimension)
        if not np.isfinite(matrix).all() or np.any(np.linalg.norm(matrix, axis=1) == 0):
            raise ValueError('Cosine index input requires finite, nonzero vectors')
    fragments = [f.fragment_id for f in ds.get_fragments()]
    if len(fragments) > 100_000:
        raise ValueError('More than 100000 fragments; compact the table before building this index')
    segment = ds.create_index_uncommitted(column, cfg['index_type'], name=cfg['name'],
        metric=cfg['metric'], replace=bool(existing), num_partitions=cfg['num_partitions'],
        m=cfg['m'], ef_construction=cfg['ef_construction'], sample_rate=cfg['sample_rate'],
        max_iterations=cfg['max_iterations'], fragment_ids=fragments, index_uuid=request['uuid'],
        shuffle_partition_batches=8, shuffle_partition_concurrency=1, filter_nan=False)
    if str(segment.uuid) != request['uuid'] or set(segment.fragment_ids) != set(fragments):
        raise RuntimeError('Staged index identity or fragment coverage differs')
    index_bytes = sum(p.stat().st_size for p in (Path(uri) / '_indices' / request['uuid']).rglob('*') if p.is_file())
    if index_bytes > cfg['max_index_bytes']:
        raise ValueError('Built index exceeds max_index_bytes')
    removed = [Index(uuid=i['uuid'], name=i['name'], fields=segment.fields,
        dataset_version=i['version'], fragment_ids=i['fragment_ids'], index_version=0) for i in existing]
    return {**base, 'action': 'rebuild' if existing else 'build', 'segment': segment,
            'removed': removed, 'contract': contract, 'index_bytes': index_bytes,
            'fragments_sha256': hashlib.sha256(json.dumps(sorted(fragments)).encode()).hexdigest()}


def ensure_lance_vector_index(uri, *, expected_version, vector_column='embedding', options=None):
    """Return a verified indexed snapshot; stage without commits, then publish once.

    Existing matching complete indices are reused. Changed parameters or incomplete
    coverage trigger a complete replacement build; existing snapshots remain valid.
    Empty tables return status=empty (never ready). Local Linux tables only.
    """
    cfg = vector_index_config(options)
    uri = normalize_lance_uri(uri)
    if not uri.startswith('/'):
        raise ValueError('Managed index construction currently requires a local table')
    if type(expected_version) is not int or expected_version < 1:
        raise ValueError('A fixed positive expected_version is required')
    if not isinstance(vector_column, str) or not vector_column:
        raise ValueError('vector_column must be a column name')
    if open_lance_dataset(uri, None).version != expected_version:
        raise LanceWriteConflict('Index target differs from expected_version')
    resources = _resource_options(cfg)
    capacity = {'threads': resources.threads, 'rss': resources.max_rss_bytes}
    group = hashlib.sha256(Path('/proc/self/cgroup').read_bytes() + json.dumps(capacity).encode()).hexdigest()[:16]
    slots = Path(tempfile.gettempdir()) / f'demiflow-index-{os.getuid()}-{group}'
    index_uuid = str(uuid.uuid4())
    request = dict(uri=uri, version=expected_version, column=vector_column, config=cfg,
                   resources=cfg, uuid=index_uuid)
    record = _record_path(uri, index_uuid)
    record.parent.mkdir(parents=True, exist_ok=True)
    commit_attempted = committed_index = False
    try:
        with DataFusionSession(resource_directory=slots, options=resources,
                diagnostics_directory=control_directory(uri) / 'index_jobs') as session:
            prepared = session._prepare_lance_index(request)
            version = expected_version
            if prepared['action'] in {'build', 'rebuild'}:
                if open_lance_dataset(uri, None).version != expected_version:
                    raise LanceWriteConflict('Index target changed while building')
                _save(record, {'uuid': index_uuid, 'contract': prepared['contract'],
                    'source_version': expected_version, 'rows': prepared['rows'],
                    'fragments_sha256': prepared['fragments_sha256']})
                from .mutate import _VersionGuard
                from .storage import lance_commit_conflict_error
                lance = require_lance()
                guard = _VersionGuard(expected_version)
                try:
                    commit_attempted = True
                    committed = lance.LanceDataset.commit(uri,
                        lance.LanceOperation.CreateIndex([prepared['segment']], prepared['removed']),
                        read_version=expected_version, max_retries=0, commit_lock=guard)
                except Exception as error:
                    if guard.conflict:
                        raise guard.conflict from error
                    if isinstance(error, lance_commit_conflict_error()):
                        raise LanceWriteConflict(str(error)) from error
                    raise
                committed_index = True
                version = committed.version
                checked = session._prepare_lance_index({**request, 'version': version, 'inspect_only': True})
            else:
                checked = prepared
            if open_lance_dataset(uri, None).version != version:
                raise LanceWriteConflict('Index target changed before returning the verified reference')
            return dict(uri=uri, version=version, source_version=expected_version,
                status='empty' if prepared['action'] == 'empty' else 'ready',
                name=cfg['name'], index_type=cfg['index_type'], column=vector_column,
                metric=cfg['metric'], parameters=prepared['parameters'],
                rows=prepared['rows'], coverage=checked['coverage'], index_uuids=checked['indices'],
                action=prepared['action'], reused=prepared['action'] == 'reuse',
                resource_budget=asdict(resources), max_index_bytes=cfg['max_index_bytes'])
    except BaseException as error:
        # The native commit can succeed before a caller is interrupted. Never
        # delete a published segment; if inspection itself fails, retain evidence.
        try:
            # A later writer may replace our index in head while old snapshots
            # still reference it. Ambiguous commit errors also retain files.
            published = (committed_index
                or (commit_attempted and not isinstance(error, LanceWriteConflict))
                or any(i['uuid'] == index_uuid for i in open_lance_dataset(uri, None).list_indices()))
        except Exception:
            published = True
        if not published:
            shutil.rmtree(Path(uri) / '_indices' / index_uuid, ignore_errors=True)
            record.unlink(missing_ok=True)
        raise
