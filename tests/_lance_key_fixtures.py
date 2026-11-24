"""Observe the current key relation and hydration in actual worker processes."""
import os
from pathlib import Path


def install_lance_key_probe(tmp_path, monkeypatch, *, fail_relation=False):
    directory = Path(tmp_path) / 'lance-key-worker-probe'
    directory.mkdir()
    path = Path(tmp_path) / 'lance-key.jsonl'
    script = '''import os
if 'DEMIFLOW_WORKER_CONTROL_FD' in os.environ:
    import lance
    from demiflow.execution.sources import source_readers as lance_key_source
    from demiflow.execution.kernels import lance_keys, join_spool
    from demiflow.operators import join_spool as spool_algorithm
    from contextvars import ContextVar
    bound_services = ContextVar("test_key_relation_services")
    from _worker_event_fixtures import emit
    key_rows = lance_key_source.key_rows
    append = spool_algorithm.JoinSpool.append
    build = lance_keys.build_sort_runs
    take_rows = lance.LanceDataset._take_rows
    pairs = join_spool.join_pairs
    def observed_keys(reader):
        count = 0
        try:
            for fragment, offset, row in key_rows(reader):
                assert set(row) == set(reader._plan.query.columns) | {'_rowid'}
                count += 1
                yield fragment, offset, row
        finally:
            emit(LOG_PATH, 'key_scan', rows=count, version=reader._version,
                 columns=list(reader._plan.query.columns))
    def observed_append(spool, record, fields, **kwargs):
        services = bound_services.get()
        row = join_spool.materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
        assert set(row) == set(fields), 'wide payload entered the key relation'
        return append(spool, record, fields, **kwargs)
    def observed_build(records, *, spec, services):
        count = 0
        def checked():
            nonlocal count
            for record in records:
                row = join_spool.materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
                assert set(row) == {'ordinal', 'row_id'}
                count += 1
                yield record
        refs = build(checked(), spec=spec, services=services)
        emit(LOG_PATH, 'key_order', rows=count, runs=len(refs))
        return refs
    def observed_take(dataset, ids, *args, **kwargs):
        result = take_rows(dataset, ids, *args, **kwargs)
        emit(LOG_PATH, 'hydrate', rows=result.num_rows, requested=len(ids), version=dataset.version)
        return result
    def observed_pairs(*args, **kwargs):
        emit(LOG_PATH, 'relation', mode=kwargs['how'])
        if FAIL_RELATION:
            raise RuntimeError('native test failure')
        token = bound_services.set(kwargs['services'])
        try:
            yield from pairs(*args, **kwargs)
        finally:
            bound_services.reset(token)
    lance_key_source.key_rows = observed_keys
    spool_algorithm.JoinSpool.append = observed_append
    lance_keys.build_sort_runs = observed_build
    lance.LanceDataset._take_rows = observed_take
    join_spool.join_pairs = observed_pairs
'''.replace('LOG_PATH', repr(str(path))).replace('FAIL_RELATION', repr(fail_relation))
    (directory / 'sitecustomize.py').write_text(script)
    monkeypatch.setenv('PYTHONPATH', str(directory) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    return path
