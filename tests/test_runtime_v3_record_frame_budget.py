from demiflow.execution.contracts import record_codec
from demiflow.execution.storage.streams import read_document
"""Payload limits do not charge provenance and JSON escaping as business data."""
import math
import hashlib
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.record_codec import _encode_payload
from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity, document
from demiflow.execution.contracts import record_codec as frames
from test_runtime_v3_c import bounds


def _record(row, position=None):
    encoded = _encode_payload(row, max_bytes=65536)
    return Record(hashlib.sha256(encoded).hexdigest(), position or {}, None, row, len(encoded), 0)


def _rows():
    return [{'k': index, 'value': ('"\\' * 750, b'\x00\xff', -0.0)} for index in range(3)]


def test_real_small_budget_typed_rows_keep_payload_bound(tmp_path):
    rows = _rows()
    assert all(3000 < len(_encode_payload(row, max_bytes=4096)) < 4096 for row in rows)
    limits = {'memory_bytes': 1024 * 1024, 'max_row_bytes': 4096,
              'max_batch_bytes': 8192, 'max_state_bytes': 32768,
              'checkpoint_rows': 1, 'checkpoint_interval_ms': 60000}
    with RunContext.start(tmp_path / 'run', limits=limits) as run:
        with data.local_execution(workers=2):
            actual = data.from_items(rows).map(lambda row: row).take_all()
        assert actual == rows
        assert all(isinstance(row['value'], tuple) and math.copysign(1, row['value'][2]) == -1 for row in actual)
        cuts = run.journal.execution_commits('action-1')
        assert cuts and all(cut['status'] == 'acked' for cut in cuts)
        assert any(not cut['intent'].get('final', False) for cut in cuts[:-1])


@pytest.mark.parametrize('value', ['"\\' * 960, 'x' * 3950, ('a', b'\x00\xff', -0.0),
                                  {'list':[1, None, True], 'tuple':(2,3)}, '测试🙂' * 300])
def test_old_document_and_line_bytes_remain_identical_and_payload_measured(value):
    from demiflow.execution.contracts.record_codec import _encode_record_document, _decode_record_document
    rec = _record({'value':value}, {'source':'s', 'offset':12})
    b = bounds(max_row_bytes=4096, max_control_bytes=8192, max_resident_bytes=256*1024)
    old = _encode_record_document(rec, max_bytes=65536)
    raw = frames.encode_document(rec, b)
    assert raw == old
    size, chunks = frames.frame_parts(rec, b)
    line = b''.join(chunks)
    assert len(line) == size
    assert line == document({'document':old.decode(), 'record_id':rec.record_id, 'sequence':0}) + b'\n'
    reread, fields = frames.decode_frame(line, b)
    decoded = frames.decode_document(reread, record_id=fields['record_id'], sequence=fields['sequence'], bounds=b)
    assert thaw_record_value(decoded.inline) == {'value':value}
    assert decoded.encoded_bytes == len(_encode_payload({'value':value}, max_bytes=4096))
    if len(raw) > b.max_row_bytes:
        with pytest.raises(DataflowError): _decode_record_document(raw, record_id=rec.record_id, sequence=0, max_bytes=4096)


@pytest.mark.parametrize('damage', ['payload', 'metadata', 'workspace', 'spoofed_size'])
def test_encode_rejects_before_building_document(monkeypatch, damage):
    from demiflow.execution.exchange import dataflow_bridge as codec
    b = bounds(max_row_bytes=4096, max_control_bytes=2048, max_resident_bytes=65536)
    rec = _record({'value':'x' * 100})
    if damage in ('payload', 'spoofed_size'):
        rec = _record({'value':'x' * 4051})
        assert rec.encoded_bytes == 4097
        if damage == 'spoofed_size': rec = replace(rec, encoded_bytes=1)
    elif damage == 'metadata': rec = replace(rec, source_position={'huge':'x'*2048})
    else: b = replace(b, max_resident_bytes=128)
    monkeypatch.setattr(record_codec, '_encode_record_document', lambda *a, **k: pytest.fail('unsafe codec allocation occurred'))
    with pytest.raises(DataflowError, match='budget|bound'):
        frames.encode_document(rec, b)


def test_decoder_separates_document_and_actual_payload_and_admits_before_parse(monkeypatch):
    from demiflow.execution.exchange import dataflow_bridge as codec
    rec = _record({'value':'x'*4050})
    raw = record_codec._encode_record_document(rec, max_bytes=65536)
    b = bounds(max_row_bytes=4096, max_resident_bytes=65536)
    assert len(raw) > 4096 and rec.encoded_bytes == 4096
    assert frames.decode_document(raw, record_id=rec.record_id, sequence=0, bounds=b) == rec
    bad = raw.replace(b'"eb":' + str(rec.encoded_bytes).encode(), b'"eb":1')
    with pytest.raises(DataflowError, match='size mismatch'):
        frames.decode_document(bad, record_id=rec.record_id, sequence=0, bounds=b)
    monkeypatch.setattr(record_codec, '_codec_loads', lambda *a, **k: pytest.fail('parse before resident admission'))
    with pytest.raises(DataflowError, match='workspace'):
        frames.decode_document(raw, record_id=rec.record_id, sequence=0, bounds=replace(b, max_resident_bytes=len(raw)+1))


def test_real_nonfinal_ack_resume_and_stored_handle_keep_wide_typed_rows(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    import json
    import time
    rows = [{**_rows()[0], 'k':i} for i in range(4)]
    def business(row):
        time.sleep(.02)
        return row
    def flow(): return data.from_items(rows).map(business)
    limits = {'memory_bytes':1024*1024, 'max_row_bytes':4096, 'max_batch_bytes':8192,
              'max_state_bytes':32768, 'checkpoint_rows':1, 'checkpoint_interval_ms':60000}
    original = DataflowCheckpointCoordinator.commit
    prefixes = []
    class Interrupted(Exception): pass
    def stop(coordinator, cut_id, *, sinks, native_queue_state=None):
        image = original(coordinator, cut_id, sinks=sinks, native_queue_state=native_queue_state)
        counts = [json.loads(b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes)))['manifest']['total_rows']
                  for ref in image.confirmed_sinks.values()]
        count = sum(counts)
        if count and not image.cut.final and not prefixes:
            prefixes.append(count)
            raise Interrupted('real acknowledged prefix')
        return image
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(workers=2):
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                flow().take_all()
    assert len(prefixes) == 1 and 0 < prefixes[0] < len(rows)
    with RunContext.resume(reference) as run, data.local_execution(workers=2):
        before = run.journal.execution_commits('action-1')
        assert before and all(c['status']=='acked' for c in before)
        assert not StateArtifacts(run).read(before[-1]['prepared']['state']['dataflow-cut-v3'])['cut']['final']
        assert flow().take_all() == rows
        after = run.journal.execution_commits('action-1')
        assert after[:len(before)] == before
        assert StateArtifacts(run).read(after[-1]['prepared']['state']['dataflow-cut-v3'])['cut']['final']
    with RunContext.start(tmp_path/'stored', limits=limits), data.local_execution(workers=2):
        saved = data.from_items(rows).materialize()
        try: assert saved.take_all() == rows
        finally: saved.release()


def test_codec_credit_retry_does_not_advance_route_or_identity():
    from demiflow.execution.exchange.dataflow_channels import _route_record_document
    from demiflow.execution.exchange.dataflow_state import RoutingCounters
    from test_runtime_v3_finite_borrow import fixture
    service, spec = fixture(None, routing='rebalance')
    record = _record({'value':'x'*256})
    counters = RoutingCounters(values={}, route_epoch=spec.route_epoch)
    with pytest.raises(DataflowError, match='workspace'):
        _route_record_document(record, spec=spec, state=counters, bounds=service.bounds,
                               retained=service.bounds.max_resident_bytes)
    assert counters.values() == {}
    actual, _ = _route_record_document(record, spec=spec, state=counters, bounds=service.bounds)
    expected, _ = _route_record_document(record, spec=spec,
        state=RoutingCounters(values={}, route_epoch=spec.route_epoch), bounds=service.bounds)
    assert actual == expected and counters.values() == {spec.channel_id:1}


def test_real_near_row_limit_scalar_uses_bounded_old_frame(tmp_path):
    row = {'value':'"\\' * 1000}
    encoded = _encode_payload(row, max_bytes=4096)
    assert 4000 <= len(encoded) <= 4096
    with RunContext.start(tmp_path/'run', limits={'memory_bytes':1024*1024,
            'max_row_bytes':4096,'max_batch_bytes':8192,'max_state_bytes':32768}), data.local_execution(workers=2):
        assert data.from_items([row]).size_bytes() == len(encoded)


def test_source_pending_keeps_old_bytes_and_restores_actual_near_limit_payload(tmp_path):
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_s import make_snapshot, first_split, make_services
    from demiflow.data.sources import ItemsSource
    from demiflow.execution.sources.source_readers import ItemsReader
    row = {'value':'x'*4050}
    with store_fixture(tmp_path) as (_,store,_):
        b = bounds(max_row_bytes=4096, max_batch_bytes=8192, max_control_bytes=8192)
        plan = ItemsSource((row,))
        snapshot = make_snapshot(plan, store, bounds=b)
        services = make_services(store, plan, snapshot.source_id, bounds=b)
        reader = ItemsReader(snapshot, services=services)
        reader.open(first_split(snapshot, plan, bounds=b), None)
        first = reader.next_record().record
        cursor = reader.snapshot()
        ref = cursor.pending_refs[0]
        saved = b''.join(store.read(ref, max_bytes=ref.size_bytes))
        raw = frames.encode_document(first, b)
        assert saved == document({'record_id':first.record_id,'sequence':0,'document':raw.decode()})
        restored = ItemsReader(snapshot, services=services)
        restored.open(first_split(snapshot, plan, bounds=b), cursor)
        try:
            assert restored.next_record().record == first
            assert restored.next_record().status == 'eoi'
        finally:
            restored.close(); reader.close()


def test_real_ray_core_near_limit_frame_preserves_original_ack_and_no_ray_data(tmp_path):
    import tempfile
    ray = pytest.importorskip('ray')
    from pathlib import Path
    from demiflow.data.api import DataAPI
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import _RayDataBlocker
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    assert Path(ray.__file__).is_file() and not ray.is_initialized()
    executor = RayDatasetExecutor(workers=2, resource_root=tmp_path)
    rows = _rows()
    with tempfile.TemporaryDirectory(prefix='demiflow-frame-ray-', dir='/tmp') as scratch, _RayDataBlocker() as blocked:
        try:
            ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                     object_store_memory=128*1024**2, _temp_dir=scratch)
            with RunContext.start(tmp_path/'run', limits={'memory_bytes':1024*1024,
                    'max_row_bytes':4096,'max_batch_bytes':8192,'max_state_bytes':32768}) as run:
                assert DataAPI(executor).from_items(rows).map(lambda row:row).take_all() == rows
                assert load_sealed_plan(run.directory,'action-1',version='v3')['physical'].policy.provider == 'ray_core'
                assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            assert not blocked.attempts
        finally:
            executor.close(); ray.shutdown()
        assert not ray.is_initialized()


def test_zero_dimensional_numpy_array_keeps_exact_payload_limit():
    np = pytest.importorskip('numpy')
    for value in (np.array(1), np.int64(1), np.array([1])):
        record = _record({'value':value})
        b = bounds(max_row_bytes=record.encoded_bytes, max_resident_bytes=65536)
        assert frames._typed_shape(record.inline, record.encoded_bytes)[0] == record.encoded_bytes
        raw = frames.encode_document(record, b)
        decoded = frames.decode_document(raw, record_id=record.record_id, sequence=0, bounds=b)
        actual = decoded.inline['value']
        assert type(actual) is type(value) and actual.shape == value.shape
        assert np.array_equal(actual, value)


def test_binary_codec_workspace_covers_decode_and_canonical_copies(monkeypatch):
    import tracemalloc
    np = pytest.importorskip('numpy')
    from demiflow.execution.exchange import dataflow_bridge as codec
    for value in (b'x'*20000, np.arange(20000,dtype=np.uint8), 'x'*20000,
                  (b'x'*20000, 'y'*10000)):
        record = _record({'value':value})
        raw = record_codec._encode_record_document(record, max_bytes=1024*1024)
        kwargs = dict(record_id=record.record_id, sequence=0, max_bytes=1024*1024,
                      payload_max_bytes=1024*1024, metadata_max_bytes=1024*1024)
        record_codec._decode_record_document(raw, **kwargs)  # Warm imports, outside accounting.
        tracemalloc.start()
        try:
            record_codec._decode_record_document(raw, **kwargs)
            _, peak = tracemalloc.get_traced_memory()
        finally: tracemalloc.stop()
        assert peak + len(raw) <= frames.raw_workspace(raw)
        old_grant = 3*len(raw) + 1024
        b = bounds(max_row_bytes=65536, max_resident_bytes=old_grant)
        with monkeypatch.context() as patch:
            patch.setattr(record_codec, '_codec_loads', lambda *a,**k: pytest.fail('parse before admission'))
            with pytest.raises(DataflowError, match='workspace'):
                frames.decode_document(raw, record_id=record.record_id, sequence=0,bounds=b)


def test_kernel_retained_credit_cannot_be_waited_away():
    import threading
    from types import SimpleNamespace
    from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
    runner = SimpleNamespace(bounds=bounds(timeout_ms=1), services=SimpleNamespace(check_stop=lambda:None),
        _condition=threading.Condition(), _wake_generation=0, _peer=SimpleNamespace(_bytes=100),
        e=SimpleNamespace(_kernels={'one':SimpleNamespace(retained_resident_bytes=200)}))
    observed=[]
    def blocked(*,resident_bytes,retained):
        observed.append(retained)
        frames._admit(retained+100,250)
    with pytest.raises(DataflowError, match='workspace'):
        WorkerGraphRunner._frame_call(runner, blocked)
    assert observed == [300]  # Even all 100 peer bytes freed leaves 300 > 250.


def test_binary_preparse_charge_survives_noncanonical_field_order_and_tag_escapes():
    from demiflow.execution.exchange import dataflow_bridge as codec
    record = _record({'value':b'x'*20000})
    raw = record_codec._encode_record_document(record, max_bytes=65536)
    encoded = b'eHh4' * (20000 // 3) + b'eHg='
    leaf = b'{"t":"bytes","v":"' + encoded + b'"}'
    assert leaf in raw
    reordered = raw.replace(leaf, b'{"v":"' + encoded + b'","t":"bytes"}')
    escaped = raw.replace(b'"t":"bytes"', b'"t":"\\u0062ytes"')
    assert frames.raw_workspace(reordered) >= frames.raw_workspace(raw)
    assert frames.raw_workspace(escaped) >= frames.raw_workspace(raw)
    # A two-byte binary leaf must not charge an unrelated wide escaped string
    # as a second base64 copy (same row/profile as the real materialize case).
    mixed = record_codec._encode_record_document(_record(_rows()[0]), max_bytes=65536)
    assert frames.raw_workspace(mixed) < 18432


def test_stored_document_read_admits_before_legacy_store_and_keeps_chunk_bound(monkeypatch):
    from types import SimpleNamespace
    record = _record({'value':'x'*4050})
    b = bounds(max_row_bytes=4096,max_resident_bytes=65536)
    raw = frames.encode_document(record,b)
    ref = SimpleNamespace(size_bytes=len(raw))
    calls=[]
    class Store:
        def read(self, ref, *, max_bytes):
            calls.append(max_bytes)
            # Legacy protocol may return arbitrary fragmentation; helper does
            # not collect an unbounded list of tiny chunks.
            yield from (raw[index:index+1] for index in range(len(raw)))
    assert read_document(ref,Store(),b) == raw
    assert len(calls) == 1
    with pytest.raises(DataflowError,match='workspace'):
        read_document(ref,Store(),replace(b,max_resident_bytes=len(raw)+1))
    assert len(calls) == 1
