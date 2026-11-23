"""Explicit shuffle seeds keep the same value order across native actions."""
from collections import Counter
from dataclasses import replace
import hashlib

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


ROWS = [{'a': i % 3, 'b': i, 'v': float(i)} for i in range(30)]


@pytest.mark.parametrize('reuse_shuffle', [False, True])
def test_explicit_seed_repeats_the_original_thirty_row_order(tmp_path, reuse_shuffle):
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        source = data.from_items(ROWS)
        shuffled = source.random_shuffle(seed=7)
        first = shuffled.take_all()
        second = (shuffled if reuse_shuffle else source.random_shuffle(seed=7)).take_all()
    assert Counter(row['b'] for row in first) == Counter(row['b'] for row in ROWS)
    # Preserve the original failing ordered assertion; multiplicity alone is insufficient.
    assert [row['b'] for row in first] == [row['b'] for row in second]


def test_seed_repeats_union_flatmap_multiplicity_despite_input_order(tmp_path):
    def expand(row):
        return [{'v': row['id'] % 5}, {'v': row['id'] % 5}]

    with RunContext.start(tmp_path / 'run'), data.local_execution(workers=2, partitions=3):
        source = data.range(30).flat_map(expand)
        first = source.union(source).random_shuffle(seed=7, num_blocks=2).take_all()
        alternate = data.from_items([{'v': i % 5} for i in reversed(range(30)) for _ in range(4)])
        second = alternate.random_shuffle(seed=7, num_blocks=4).take_all()
    assert Counter(row['v'] for row in first) == {i: 24 for i in range(5)}
    assert first == second
    # Every value appears in multiple runs, rather than one digest-equal group.
    assert sum(first[i]['v'] != first[i + 1]['v'] for i in range(len(first) - 1)) > 10


def _record(row, index, action='a', *, max_bytes=4096):
    from demiflow.execution.contracts.dataflow import Record
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    return Record(hashlib.sha256(f'{action}:{index}'.encode()).hexdigest(),
        {'source': action, 'ordinal': index}, None, row,
        len(_encode_payload(row, max_bytes=max_bytes)), index)


def _services(store, port, *, stop=lambda: None):
    from test_runtime_v3_c import make, bounds
    return make('RuntimeServices', store=store, journal=port, check_stop=stop,
        bounds=bounds(max_resident_bytes=1 << 20, max_row_bytes=4096,
                      max_batch_bytes=32768, max_open_files=12, max_spill_bytes=16 << 20))


def _spec(*, version=True, occurrence='shuffle-a'):
    from test_runtime_v3_c import make
    config = {'seed': 7, 'occurrence_id': hashlib.sha256(occurrence.encode()).hexdigest()}
    if version:
        config['shuffle_order_version'] = 'typed-multiset-v1'
    return make('KernelSpec', kind='RandomShuffleOp', configuration=config,
        state='none', effect='pure', resource_role='cpu', mergeable=False,
        output_bound_bytes=4096)


def test_low_window_real_store_spills_twice_and_closes_every_reader(tmp_path, monkeypatch):
    from demiflow.execution.kernels.shuffle_order import seeded_shuffle, shuffle_windows
    from test_runtime_v3_artifact_accounting import store_fixture, consumed
    source = [{'v': i % 23, 'payload': b'x' * 256} for i in range(300)]
    with store_fixture(tmp_path, ceiling=16 << 20) as (run, store, port):
        services = _services(store, port)
        windows = shuffle_windows(services)
        assert windows.input_bytes + windows.sort.run_bytes + windows.sort.codec_bytes + windows.metadata_bytes <= windows.resident_bytes
        assert windows.sort.merge_resident_bytes + windows.sort.run_bytes + windows.sort.codec_bytes + windows.metadata_bytes <= windows.resident_bytes
        original_put, original_read, original_release = store.put, store.read, store.release
        written, released, active = [], [], [0]
        def put(*args, **kwargs):
            ref = original_put(*args, **kwargs)
            written.append(ref)
            return ref
        def read(*args, **kwargs):
            active[0] += 1
            chunks = original_read(*args, **kwargs)
            try:
                yield from chunks
            finally:
                chunks.close()
                active[0] -= 1
        def release(ref, **kwargs):
            released.append(ref)
            return original_release(ref, **kwargs)
        monkeypatch.setattr(store, 'put', put)
        monkeypatch.setattr(store, 'read', read)
        monkeypatch.setattr(store, 'release', release)
        one = list(seeded_shuffle((_record(row, i) for i, row in enumerate(source)),
            spec=_spec(), services=services))
        assert len(written) > 4  # Both build passes plus finite-fan-in merges.
        assert set(written) <= set(released)
        assert active == [0]
        two = list(seeded_shuffle((_record(row, i, 'another-action') for i, row in enumerate(reversed(source))),
            spec=_spec(occurrence='shuffle-b'), services=services))
        assert [r.inline['v'] for r in one] == [r.inline['v'] for r in two]
        assert Counter(r.inline['v'] for r in one) == Counter(r['v'] for r in source)
        assert set(written) <= set(released)
        assert active == [0]
        assert consumed(run) <= 16 << 20
        assert not list((store.root / store.store_id / 'staging').iterdir())


def test_duplicate_record_identity_order_survives_reordered_delivery(tmp_path):
    import random
    from demiflow.execution.kernels.shuffle_order import seeded_shuffle
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    from test_runtime_v3_artifact_accounting import store_fixture
    records = [_record({'v': i % 3}, i) for i in range(90)]
    # Distinct provenance also disambiguates a forwarded record-id collision.
    records.append(replace(records[0], source_position={'source': 'a', 'ordinal': 900}, sequence=900))
    reordered = list(records)
    random.Random(19).shuffle(reordered)
    with store_fixture(tmp_path) as (_, store, port):
        services = _services(store, port)
        def output(items):
            return [(r.record_id, dict(r.source_position), _encode_payload(r.inline, max_bytes=4096))
                for r in seeded_shuffle(iter(items), spec=_spec(), services=services)]
        assert output(records) == output(reversed(records)) == output(reordered)


def _operator_spec():
    from test_runtime_v3_c import make
    return replace(_spec(), inputs=(make('InputPort', name='main', requirement='finite_complete', ordering='global'),))


def test_actual_operator_owned_spool_fills_old_segment_under_original_budget(tmp_path):
    from demiflow.execution.kernels.operator import OperatorKernel, _SpoolPort
    from demiflow.execution.kernels.shuffle_order import shuffle_windows
    from test_runtime_v3_artifact_accounting import store_fixture
    records = [_record({'v': i % 23, 'payload': 'x' * 350}, i) for i in range(1000)]
    with store_fixture(tmp_path, ceiling=16 << 20) as (_, store, port):
        services = _services(store, port)
        services = replace(services, bounds=replace(services.bounds, max_batch_bytes=1 << 20))
        windows = shuffle_windows(services)
        legacy = _SpoolPort('legacy', services, 8192)
        kernel = OperatorKernel(_operator_spec(), services=services, partition_id='p0')
        try:
            kernel.start()
            for record in records:
                legacy.push(record)
                assert list(kernel.push('main', record)) == []
            legacy.finish()
            assert max(ref.size_bytes for ref in legacy._refs) > (windows.input_bytes - 4096) // 2
            emitted = kernel.finish('main')
            try:
                first = next(emitted)
                assert kernel.retained_resident_bytes > 0  # Exercise the actual getter/callback.
                output = [first, *emitted]
            finally:
                emitted.close()
            assert kernel.retained_resident_bytes == 0
            assert Counter(r.record_id for r in output) == Counter(r.record_id for r in records)
            assert len(kernel._ports['main']._refs) > len(legacy._refs)
            assert services.bounds.max_resident_bytes == services.bounds.max_batch_bytes == 1 << 20
        finally:
            kernel.close()
            legacy.release()


def test_new_shuffle_keeps_original_wide_row_success_budget(tmp_path):
    from demiflow.execution.kernels.operator import OperatorKernel
    from test_runtime_v3_artifact_accounting import store_fixture
    records = [_record({'v': i, 'payload': 'x' * 16384}, i, max_bytes=32768) for i in range(12)]
    with store_fixture(tmp_path, ceiling=16 << 20) as (_, store, port):
        services = _services(store, port)
        services = replace(services, bounds=replace(services.bounds,
            max_row_bytes=32768, max_batch_bytes=1 << 20))
        for version in (False, True):
            spec = replace(_operator_spec(), configuration=_spec(version=version).configuration,
                output_bound_bytes=32768)
            kernel = OperatorKernel(spec, services=services, partition_id='p0')
            try:
                kernel.start()
                for record in records:
                    list(kernel.push('main', record))
                output = list(kernel.finish('main'))
                assert Counter(r.record_id for r in output) == Counter(r.record_id for r in records)
                assert all(r.inline['payload'] == 'x' * 16384 for r in output)
                assert services.bounds.max_resident_bytes == 1 << 20
            finally:
                kernel.close()


@pytest.mark.parametrize('run_lengths', [(3,), (1, 3)])
def test_measured_merge_drops_old_wide_frames_at_single_reader_tail(tmp_path, monkeypatch, run_lengths):
    import weakref
    from demiflow.execution.kernels import ordering
    from demiflow.execution.kernels.shuffle_order import shuffle_windows, _TypedKeys
    from test_runtime_v3_artifact_accounting import store_fixture

    class ObservedFrame:
        # Track the actual yielded frame owner without changing its contents.
        def __init__(self, value):
            self.value = value
        def __getitem__(self, index):
            return self.value[index]

    with store_fixture(tmp_path, ceiling=16 << 20) as (_, store, port):
        services = _services(store, port)
        services = replace(services, bounds=replace(services.bounds,
            max_row_bytes=32768, max_batch_bytes=1 << 20))
        window = shuffle_windows(services).sort
        refs, observed = [], []
        original = ordering._run_frames
        def watch(*args, **kwargs):
            source = original(*args, **kwargs)
            try:
                for value in source:
                    frame = ObservedFrame(value)
                    observed.append(weakref.ref(frame))
                    yield frame
                    del frame
            finally:
                source.close()
        start = 0
        try:
            for count in run_lengths:
                records = (_record({'v': i, 'payload': 'x' * 16384}, i, max_bytes=32768)
                    for i in range(start, start + count))
                built = ordering._build_sort_runs(records, spec=_spec(), services=services,
                    window=window, key_for_record=_TypedKeys(services, window))
                refs.extend(built)
                assert len(built) == 1
                start += count
            monkeypatch.setattr(ordering, '_run_frames', watch)
            merged = ordering._merge_into(tuple(refs), services, window)
            count = 0
            try:
                for frame in merged:
                    count += 1
                    # The stdlib fast path retains the first tail frame and,
                    # after a two-to-one transition, an exhausted entry too.
                    assert sum(ref() is not None for ref in observed) <= len(refs)
                    del frame
            finally:
                merged.close()
            assert count == sum(run_lengths)
            assert not any(ref() is not None for ref in observed)
        finally:
            ordering._release_runs(refs, services)


def test_two_shuffle_passes_share_the_original_spill_ceiling(tmp_path, monkeypatch):
    from demiflow.execution.kernels import shuffle_order
    from demiflow.execution.run_journal import ExecutionBudgetExceeded
    from test_runtime_v3_artifact_accounting import store_fixture, consumed
    with store_fixture(tmp_path, ceiling=256 << 10) as (run, store, port):
        services = _services(store, port)
        services = replace(services, bounds=replace(services.bounds, max_spill_bytes=256 << 10))
        builds, written, released = [], [], []
        original_build, original_put, original_release = shuffle_order._build_sort_runs, store.put, store.release
        def build(*args, **kwargs):
            builds.append(True)
            return original_build(*args, **kwargs)
        def put(*args, **kwargs):
            ref = original_put(*args, **kwargs)
            written.append(ref)
            return ref
        def release(ref, **kwargs):
            released.append(ref)
            return original_release(ref, **kwargs)
        monkeypatch.setattr(shuffle_order, '_build_sort_runs', build)
        monkeypatch.setattr(store, 'put', put)
        monkeypatch.setattr(store, 'release', release)
        records = (_record({'v': i, 'payload': b'x' * 256}, i) for i in range(100))
        with pytest.raises(ExecutionBudgetExceeded) as caught:
            list(shuffle_order.seeded_shuffle(records, spec=_spec(), services=services))
        assert caught.value.budget == 'artifact_staging_bytes'
        assert len(builds) == 2
        assert written and set(written) <= set(released)
        assert consumed(run) == sum({ref.digest: ref.size_bytes for ref in written}.values()) <= 256 << 10
        assert not list((store.root / store.store_id / 'staging').iterdir())


@pytest.mark.parametrize('wide_control', ['seal', 'manifest'])
def test_borrowed_metadata_is_admitted_while_first_sort_runs_exist(tmp_path, monkeypatch, wide_control):
    import json
    from dataclasses import asdict
    from demiflow.execution.contracts.dataflow import ProducerCompletion
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_finite_input import borrowed_records
    from demiflow.execution.storage.segments import FINITE_RECORDS_MEDIA, SEGMENTS_MEDIA, SCHEMA
    from demiflow.execution.exchange.dataflow_channels import seal_scope
    from demiflow.execution.kernels.operator import OperatorKernel
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import make
    with store_fixture(tmp_path, ceiling=16 << 20) as (run, store, port):
        services = _services(store, port)
        services = replace(services, bounds=replace(services.bounds,
            max_depth=512, max_batch_bytes=1 << 20, max_control_bytes=1 << 20))
        empty = store.put(iter(()), max_bytes=1, media_type=FINITE_RECORDS_MEDIA)
        manifest = empty
        if wide_control == 'manifest':
            child = {'ref': asdict(empty), 'rows': 0, 'bytes': 0, 'height': 0}
            raw = json.dumps({'schema': SCHEMA, 'height': 1, 'rows': 0, 'bytes': 0,
                'children': [child] * 512}, separators=(',', ':')).encode()
            manifest = store.put(iter((raw,)), max_bytes=len(raw), media_type=SEGMENTS_MEDIA)
        producers = tuple(f'p{i:04d}-' + 'x' * 120 for i in range(256 if wide_control == 'seal' else 1))
        exchange = make('ExchangeSpec', channel_id='shuffle-input', edge_id=hashlib.sha256(b'edge-shuffle').hexdigest(),
            producer_partitions=producers, consumer_partitions=('p0',), mode='finite_sealed',
            routing='forward', scope_id='shuffle-scope', route_epoch=0, storage_bytes=16 << 20)
        seal = seal_scope(exchange.scope_id, tuple(ProducerCompletion(p, 0, 'succeeded', -1, manifest)
            for p in producers), store=store)
        assignment = replace(services.assignment, inputs=(exchange,),
            lease=replace(services.assignment.lease, partition_ids=('p0',)))
        services = replace(services, assignment=assignment)
        entry = {'schema': 'demiflow.borrowed-finite-input.v1', 'seal_ref': seal,
            'manifest_ref': manifest, 'edge_id': exchange.edge_id, 'producer': producers[0],
            'target': 'p0', 'rows': 0, 'bytes': 0, 'total_rows': 0, 'total_bytes': 0}
        # The original manifest/seal is valid; the new failure must be budget
        # admission with sort runs live, not malformed synthetic metadata.
        assert list(borrowed_records(services, entry)) == []
        kernel = OperatorKernel(_operator_spec(), services=services, partition_id='p0')
        written, released = [], []
        original_put, original_release = store.put, store.release
        def put(*args, **kwargs):
            ref = original_put(*args, **kwargs)
            written.append(ref)
            return ref
        def release(ref, **kwargs):
            if kwargs['owner'] == 'kernel-sort':
                released.append(ref)
            return original_release(ref, **kwargs)
        try:
            kernel.start()
            for i in range(400):
                list(kernel.push('main', _record({'v': i, 'payload': 'x' * 256}, i)))
            kernel._ports['main'].adopt(entry)
            kernel._ports['main'].finish()
            monkeypatch.setattr(store, 'put', put)
            monkeypatch.setattr(store, 'release', release)
            with pytest.raises(DataflowError, match='workspace|subwindow') as caught:
                list(kernel.finish('main'))
            assert caught.value.code == 'BUDGET_EXCEEDED'
            assert written and set(written) <= set(released)
            assert seal not in released and manifest not in released
            assert store.inspect(seal) and store.inspect(manifest)
            assert not run.journal.execution_commits('action-1')
            assert not list((store.root / store.store_id / 'staging').iterdir())
        finally:
            kernel.close()


@pytest.mark.parametrize('boundary', ['first_build', 'second_build', 'output_close', 'disk_failure'])
def test_real_store_failure_and_close_release_only_owned_shuffle_runs(tmp_path, monkeypatch, boundary):
    from demiflow.execution.kernels import shuffle_order
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from test_runtime_v3_artifact_accounting import store_fixture
    source = [{'v': i % 23, 'payload': b'x' * 256} for i in range(300)]
    with store_fixture(tmp_path, ceiling=16 << 20) as (run, store, port):
        written, released, active = [], [], [0]
        original_put, original_release, original_read = store.put, store.release, store.read
        cancel = [False]
        def stop():
            if cancel[0]:
                raise DataflowError('CANCELLED', 'controlled shuffle cancellation')
        services = _services(store, port, stop=stop)
        def put(*args, **kwargs):
            if boundary == 'disk_failure' and written:
                raise OSError('controlled disk full')
            result = original_put(*args, **kwargs)
            written.append(result)
            if boundary == 'first_build':
                cancel[0] = True
            return result
        def release(ref, **kwargs):
            released.append(ref)
            return original_release(ref, **kwargs)
        def read(*args, **kwargs):
            active[0] += 1
            chunks = original_read(*args, **kwargs)
            try:
                yield from chunks
            finally:
                chunks.close()
                active[0] -= 1
        monkeypatch.setattr(store, 'put', put)
        monkeypatch.setattr(store, 'release', release)
        monkeypatch.setattr(store, 'read', read)
        if boundary == 'second_build':
            original = shuffle_order._build_sort_runs
            builds = [0]
            def build(records, **kwargs):
                builds[0] += 1
                if builds[0] == 2:
                    def interrupt():
                        for i, record in enumerate(records):
                            if i == 100:
                                cancel[0] = True
                            yield record
                    return original(interrupt(), **kwargs)
                return original(records, **kwargs)
            monkeypatch.setattr(shuffle_order, '_build_sort_runs', build)
        emitted = shuffle_order.seeded_shuffle((_record(row, i) for i, row in enumerate(source)),
            spec=_spec(), services=services)
        if boundary == 'output_close':
            next(emitted)
            emitted.close()
        else:
            with pytest.raises((DataflowError, OSError), match='cancellation|disk full'):
                list(emitted)
        assert written and set(written) <= set(released)
        assert active == [0]
        assert not run.journal.execution_commits('action-1')
        assert not list((store.root / store.store_id / 'staging').iterdir())


def test_typed_codec_values_and_mapping_order_contract(tmp_path):
    import datetime
    from decimal import Decimal
    import numpy as np
    from demiflow.execution.kernels.shuffle_order import seeded_shuffle
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    from test_runtime_v3_artifact_accounting import store_fixture
    rows = [{'v': value} for value in (None, True, 1, 1.0, -0.0, float('nan'), float('inf'),
            b'\x00data', (1, 'x'), [1, 'x'], {1, 2}, frozenset({1, 2}), Decimal('1.20'),
            datetime.date(2026, 11, 17), np.array([1, 2], dtype='int32'))]
    rows += [{'v': {'a': 1, 'b': 2}}, {'v': {'b': 2, 'a': 1}}]
    encoded = lambda row: _encode_payload(row, max_bytes=4096)
    assert encoded(rows[-2]) != encoded(rows[-1])
    with store_fixture(tmp_path) as (_, store, port):
        services = _services(store, port)
        first = [encoded(r.inline) for r in seeded_shuffle((_record(row, i) for i, row in enumerate(rows * 2)),
            spec=_spec(), services=services)]
        second = [encoded(r.inline) for r in seeded_shuffle((_record(row, i, 'b') for i, row in enumerate(reversed(rows * 2))),
            spec=_spec(occurrence='other'), services=services)]
    assert first == second
    assert Counter(first) == Counter(encoded(row) for row in rows * 2)


def test_legacy_formula_and_old_sealed_configuration_remain_unchanged(tmp_path, monkeypatch):
    from demiflow.execution.kernels.ordering import shuffle_key
    from demiflow.execution.action_driver import dataflow as execution_module
    from demiflow.planning import dataflow_compile
    from test_runtime_v3_real_seams import prepared
    expected = hashlib.sha256(b'["demiflow-shuffle-v3",7,"occ","rec-1"]').digest()
    assert shuffle_key('rec-1', seed=7, occurrence_id='occ') == expected
    compile_kernel = dataflow_compile.compile_kernel_spec
    def original_config(*args, **kwargs):
        result = compile_kernel(*args, **kwargs)
        return replace(result, configuration={k: v for k, v in result.configuration.items()
                                             if k != 'shuffle_order_version'})
    with monkeypatch.context() as patch:
        patch.setattr(dataflow_compile, 'compile_kernel_spec', original_config)
        with prepared(tmp_path, data.range(30).random_shuffle(seed=7)) as (execution, services):
            # Reload after the original coordinator has surrendered its lease.
            execution.close()
            patch.setattr(execution_module, 'lower_action', lambda *a, **k: pytest.fail('sealed plan was recompiled'))
            reloaded = execution_module.DataflowExecution(execution._request, services=services)
            try:
                assert reloaded.prepare() == execution._sealed
                shuffle = next(n for n in reloaded._graph.nodes if n.kernel.kind == 'RandomShuffleOp')
                assert 'shuffle_order_version' not in shuffle.kernel.configuration
            finally:
                reloaded.close()


def test_unknown_version_and_wide_frame_fail_explicitly(tmp_path):
    from demiflow.execution.kernels.shuffle_order import seeded_shuffle
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from test_runtime_v3_artifact_accounting import store_fixture
    with store_fixture(tmp_path) as (_, store, port):
        services = _services(store, port)
        invalid = replace(_spec(), configuration={'seed': 7, 'occurrence_id': 'x', 'shuffle_order_version': 'future'})
        with pytest.raises(DataflowError, match='unknown'):
            list(seeded_shuffle(iter(()), spec=invalid, services=services))
        small = replace(services, bounds=replace(services.bounds, max_resident_bytes=128 << 10))
        with pytest.raises(DataflowError, match='subwindow'):
            list(seeded_shuffle(iter([_record({'v': b'x' * 1800}, 0)]), spec=_spec(), services=small))


def test_same_typed_input_order_on_local_and_ray_core(tmp_path):
    from test_runtime_v3_provider_recovery import executor, private_ray
    results = []
    with private_ray():
        for provider in ('local', 'ray_core'):
            with executor(provider), RunContext.start(tmp_path / provider,
                    limits={'memory_bytes': 32 << 20, 'max_row_bytes': 4096,
                            'max_batch_bytes': 1 << 20}):
                source = data.range(30).map(lambda row: {
                    'a': row['id'] % 3, 'b': row['id'], 'v': float(row['id'])})
                same = source.random_shuffle(seed=7, num_blocks=2)
                first, again = same.take_all(), same.take_all()
                changed_layout = source.random_shuffle(seed=7, num_blocks=4).take_all()
                assert first == again == changed_layout
                results.append(first)
    assert results[0] == results[1]
    assert Counter(row['b'] for row in results[0]) == Counter(range(30))


def test_original_nonfinal_cut_resumes_the_same_seeded_order(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.action_driver import dataflow as execution_module
    class Interrupted(Exception):
        pass
    def dataset():
        return data.from_items(ROWS).random_shuffle(seed=7, num_blocks=2)
    limits = {'memory_bytes': 32 << 20, 'max_row_bytes': 4096, 'max_batch_bytes': 1 << 20,
              'checkpoint_interval_ms': 60000, 'checkpoint_rows': 5}
    with RunContext.start(tmp_path / 'expected', limits=limits), data.local_execution():
        expected = dataset().take_all()
    cuts = []
    finish_publication = DataflowExecution._finish_publication
    def interrupt(execution, image):
        finish_publication(execution, image)
        fact = execution._action.context.journal.execution_commit(image.commit_id)
        prefix = fact['prepared']['sinks']['take_all']['rows']
        if not image.cut.final and 0 < prefix < len(ROWS):
            assert fact['status'] == 'acked'
            cuts.append(image.commit_id)
            raise Interrupted()
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'recover', limits=limits) as run, data.local_execution():
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_finish_publication', interrupt)
                dataset().take_all()
    assert len(cuts) == 1
    with RunContext.resume(ref) as run, data.local_execution():
        original_seal = run.journal.source_binding('action-1-dataflow-sealed')
        with monkeypatch.context() as patch:
            patch.setattr(execution_module, 'lower_action', lambda *a, **k: pytest.fail('recovery recompiled the graph'))
            actual = dataset().take_all()
        assert run.journal.source_binding('action-1-dataflow-sealed') == original_seal
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert actual == expected
