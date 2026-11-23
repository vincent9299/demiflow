"""Process-visible probes for real Arrow source, transfer and sink consumers."""
import os
from pathlib import Path


def install_arrow_probe(tmp_path, monkeypatch, *, forbid_rows=True):
    directory = Path(tmp_path) / 'arrow-probe'
    directory.mkdir()
    path = Path(tmp_path) / 'arrow-events.jsonl'
    script = '''from demiflow.execution import arrow_payload as module
from demiflow.execution.kernels.arrow_payload import ArrowBatchCache
from _worker_event_fixtures import emit
if not getattr(module, '_fixture_probe_installed', False):
    module._fixture_probe_installed = True
    publish, fallback, row, load = module.publish_batch, module.fallback_row, ArrowBatchCache.row, ArrowBatchCache.load
    def publish_probe(batch, **kwargs):
        value = publish(batch, **kwargs)
        emit(LOG_PATH, 'publish', rows=batch.num_rows, carried=value is not None,
             bytes=value[0].size_bytes if value else 0, digest=value[0].digest if value else None,
             schema=batch.schema.names)
        return value
    def fallback_probe(*args, **kwargs):
        emit(LOG_PATH, 'python_fallback')
        if FORBID_ROWS:
            raise AssertionError('Pure copy must retain Arrow batches, not Python rows')
        return fallback(*args, **kwargs)
    def row_probe(*args, **kwargs):
        emit(LOG_PATH, 'python_decode')
        if FORBID_ROWS:
            raise AssertionError('Pure copy must not decode an Arrow row into Python')
        return row(*args, **kwargs)
    def load_probe(cache, ref):
        changed = ref != cache.ref
        result = load(cache, ref)
        if changed:
            emit(LOG_PATH, 'read_batch', rows=result[0].num_rows, digest=ref.digest)
        return result
    module.publish_batch, module.fallback_row = publish_probe, fallback_probe
    ArrowBatchCache.row, ArrowBatchCache.load = row_probe, load_probe
'''.replace('LOG_PATH', repr(str(path))).replace('FORBID_ROWS', repr(forbid_rows))
    # Both original process workers and Ray Python workers inherit this owned
    # test PYTHONPATH. Driver patch covers the native sink and collection path.
    (directory / 'sitecustomize.py').write_text(script)
    test_root = Path(__file__).resolve().parent
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join((str(directory), str(test_root.parent),
        str(test_root), os.environ.get('PYTHONPATH', ''))))
    from demiflow.execution import arrow_payload as module
    from demiflow.execution.kernels.arrow_payload import ArrowBatchCache
    old = {name: getattr(module, name) for name in ('publish_batch','fallback_row')}
    for name, value in old.items():
        monkeypatch.setattr(module, name, value)
    for name in ('row','load'):
        monkeypatch.setattr(ArrowBatchCache, name, getattr(ArrowBatchCache, name))
    monkeypatch.setattr(module, '_fixture_probe_installed', False, raising=False)
    exec(compile(script, str(directory / 'sitecustomize.py'), 'exec'), {})
    return path
