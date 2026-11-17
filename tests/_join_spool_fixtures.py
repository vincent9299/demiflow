"""Observe the original bounded join inside owned worker processes."""
import os
from pathlib import Path


def install_join_spool_probe(tmp_path, monkeypatch):
    directory = Path(tmp_path) / 'join-worker-probe'
    directory.mkdir()
    path = Path(tmp_path) / 'join-probe.jsonl'
    script = '''import os
if 'DEMIFLOW_WORKER_CONTROL_FD' in os.environ:
    from demiflow.execution.kernels import join_spool
    from _worker_event_fixtures import emit
    original_class, original_join = join_spool._Spool, join_spool.join_pairs
    reports = []
    class ObservedSpool(original_class):
        def __init__(self, file, services, *, bounded=False, retained_key=None):
            super().__init__(file, services, bounded=bounded, retained_key=retained_key)
            self.proof = dict(input_rows=0, hydrated_rows=0, max_document_bytes=0,
                max_payload_bytes=0, max_index_bytes=0, max_index_entries=0,
                index_limit=self.index_limit, row_window=self.row_window,
                max_resident_bytes=services.bounds.max_resident_bytes,
                max_row_bytes=services.bounds.max_row_bytes,
                max_batch_bytes=services.bounds.max_batch_bytes,
                native_limits=dict((getattr(services, 'constants', {}) or {}).get('native_limits') or {}))
            assert len(reports) < 32
            reports.append(self.proof)
        def append(self, record, fields):
            result = super().append(record, fields)
            self.proof['input_rows'] += 1
            self.proof['max_payload_bytes'] = max(self.proof['max_payload_bytes'], record.encoded_bytes)
            return result
        def record(self, offset, header):
            result = super().record(offset, header)
            self.proof['hydrated_rows'] += 1
            self.proof['max_document_bytes'] = max(self.proof['max_document_bytes'], header[3])
            return result
        def index(self, chain):
            result = super().index(chain)
            if result is not None:
                used, count = 0, 0
                for key, entries in result.items():
                    assert type(key) is bytes
                    for offset, header in entries:
                        # The actual hash index carries key/offset/header only,
                        # never the wide opaque business value or decoded row.
                        assert type(offset) is int and type(header) is tuple and len(header) == 5
                        assert all(type(value) is int for value in header)
                        used += 512 + len(key)
                        count += 1
                assert used <= self.index_limit
                self.proof['max_index_bytes'] = max(self.proof['max_index_bytes'], used)
                self.proof['max_index_entries'] = max(self.proof['max_index_entries'], count)
            return result
    def observed_join(*args, **kwargs):
        first = len(reports)
        try:
            yield from original_join(*args, **kwargs)
        finally:
            for proof in reports[first:]:
                emit(LOG, 'join', **proof)
            del reports[first:]
    join_spool._Spool = ObservedSpool
    join_spool.join_pairs = observed_join
'''.replace('LOG', repr(str(path)))
    (directory / 'sitecustomize.py').write_text(script)
    monkeypatch.setenv('PYTHONPATH', str(directory) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    return path
