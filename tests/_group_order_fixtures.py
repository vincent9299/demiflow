"""Observe key-only comparisons and bounded external runs in real workers."""
import os
from pathlib import Path


def install_group_order_probe(tmp_path, monkeypatch):
    directory = Path(tmp_path) / 'group-order-worker-probe'
    directory.mkdir()
    path = Path(tmp_path) / 'group-order.jsonl'
    script = '''import os
if 'DEMIFLOW_WORKER_CONTROL_FD' in os.environ:
    from demiflow.execution.kernels import relational, ordering
    from _worker_event_fixtures import emit
    original_build, original_write = relational.build_sort_runs, ordering._write_run
    active = []
    def observed_write(frames, services):
        if active:
            proof = active[-1]
            resident = sum(len(frame[2]) + ordering._FRAME_OVERHEAD for frame in frames)
            assert resident <= services.bounds.max_resident_bytes
            proof['max_run_resident_bytes'] = max(proof['max_run_resident_bytes'], resident)
            for key, provenance, raw in frames:
                # Only the configured group key enters comparisons. Wide
                # payload remains an opaque serialized frame, never a key.
                assert len(key) == len(proof['keys'])
                assert all(type(token) is tuple and len(token) == 2 and
                           type(token[0]) is int and type(token[1]) is bytes for token in key)
                proof['max_key_bytes'] = max(proof['max_key_bytes'], sum(len(token[1]) for token in key))
            proof['rows'] += len(frames)
        return original_write(frames, services)
    def observed_build(records, *, spec, services):
        proof = dict(keys=list(spec.configuration['keys']), rows=0,
            max_key_bytes=0, max_run_resident_bytes=0, runs=0,
            max_resident_bytes=services.bounds.max_resident_bytes,
            max_row_bytes=services.bounds.max_row_bytes,
            max_batch_bytes=services.bounds.max_batch_bytes)
        assert len(active) < 8
        active.append(proof)
        try:
            refs = original_build(records, spec=spec, services=services)
            proof['runs'] = len(refs)
            proof['spill_bytes'] = sum(ref.size_bytes for ref in refs)
            emit(LOG_PATH, 'group_sort', **proof)
            return refs
        finally:
            assert active.pop() is proof
    relational.build_sort_runs = observed_build
    ordering._write_run = observed_write
'''.replace('LOG_PATH', repr(str(path)))
    (directory / 'sitecustomize.py').write_text(script)
    monkeypatch.setenv('PYTHONPATH', str(directory) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    return path
