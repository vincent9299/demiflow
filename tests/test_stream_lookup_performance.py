"""Lookup plan changes preserve pinned rows, payload bounds and ownership."""
import asyncio
import threading

import lance
import pyarrow as pa
import pytest

from demiflow.operators.grouping import row_size
from demiflow.operators.lookup import LanceLookup


class ObservedDataset:
    def __init__(self, dataset):
        self.dataset = dataset
        self.schema = dataset.schema
        self.scans, self.takes = [], []
        self.open_readers = 0
        self.before_take = None

    def describe_indices(self):
        return self.dataset.describe_indices()

    def scanner(self, **arguments):
        self.scans.append(arguments)
        scanner, owner = self.dataset.scanner(**arguments), self

        class Scanner:
            def to_batches(self):
                reader = scanner.to_batches()
                owner.open_readers += 1
                try:
                    yield from reader
                finally:
                    reader.close()
                    owner.open_readers -= 1

        return Scanner()

    def _take_rows(self, ids, *, columns):
        self.takes.append(list(ids))
        if self.before_take is not None:
            self.before_take()
        return self.dataset._take_rows(ids, columns=columns)


async def open_lookup(monkeypatch, dataset, *, on='id', columns=None,
                      max_matches=1, max_bytes=1024**2):
    observed = ObservedDataset(dataset)
    monkeypatch.setattr(lance, 'dataset', lambda **source: observed)
    actor = LanceLookup({'uri': dataset.uri, 'version': dataset.version},
                        on, columns, 'matches', max_matches, max_bytes)
    await actor.astart()
    return actor, observed


@pytest.mark.parametrize('indexed', [False, True])
@pytest.mark.parametrize('kind,key,other', [
    (pa.string(), "引号' OR TRUE -- `", 'different'),
    (pa.int64(), -(2**61), 2**61),
    (pa.bool_(), True, False),
])
async def test_key_types_projection_and_independent_nested_results(
        tmp_path, monkeypatch, indexed, kind, key, other):
    on = 'key_field'
    payload = {'body': 'text', 'tags': ['a', None], 'child': {'n': 7}}
    dataset = lance.write_dataset(pa.table({on: pa.array([other, key], type=kind),
                                           'payload': [None, payload]}), tmp_path/'rows')
    if indexed:
        dataset.create_scalar_index(on, 'BTREE')
    actor, observed = await open_lookup(monkeypatch, dataset, on=on, columns=['payload'])
    try:
        result = await actor({on: key, 'outside': 3})
        assert result == {on: key, 'outside': 3, 'matches': [{'payload': payload}]}
        result['matches'][0]['payload']['tags'].append('changed')
        assert (await actor({on: key}))['matches'] == [{'payload': payload}]
        assert len(observed.scans) == 1
        assert all(len(ids) == 1 for ids in observed.takes)
    finally:
        await actor.aclose()
    assert observed.open_readers == 0
    assert actor.table is None and not actor.cache and actor.cache_bytes == 0


@pytest.mark.parametrize('indexed', [False, True])
@pytest.mark.parametrize('stable_ids', [False, True])
async def test_pinned_deleted_compacted_and_appended_rows(tmp_path, monkeypatch, indexed, stable_ids):
    uri = tmp_path/'rows'
    dataset = lance.write_dataset(pa.table({'id': ['a', 'deleted'], 'seq': [0, 1]}), uri,
                                  enable_stable_row_ids=stable_ids)
    if indexed:
        dataset.create_scalar_index('id', 'BTREE')
    dataset = lance.write_dataset(pa.table({'id': ['a', 'tail'], 'seq': [2, 3]}),
                                  uri, mode='append', enable_stable_row_ids=stable_ids)
    dataset.delete("id = 'deleted'")
    dataset.optimize.compact_files(target_rows_per_fragment=128)
    pinned = lance.dataset(dataset.uri, version=dataset.version)
    lance.write_dataset(pa.table({'id': ['a'], 'seq': [999]}), uri, mode='overwrite',
                        enable_stable_row_ids=stable_ids)
    actor, observed = await open_lookup(monkeypatch, pinned)
    try:
        assert (await actor({'id': 'tail'}))['matches'] == [{'id': 'tail', 'seq': 3}]
        assert (await actor({'id': 'deleted'}))['matches'] == []
        with pytest.raises(ValueError, match='match/byte'):
            await actor({'id': 'a'})
        if not indexed:
            assert observed.takes  # Exercise row-ID hydration after compaction.
    finally:
        await actor.aclose()
    assert observed.open_readers == 0


@pytest.mark.parametrize('columns', [[], ['id', 'id'], {'alias': 'value + 1'}, ['absent']])
async def test_other_projections_keep_native_validation_and_results(tmp_path, monkeypatch, columns):
    dataset = lance.write_dataset(pa.table({'id': ['hit'], 'value': [2]}), tmp_path/'rows')
    try:
        expected = [row for batch in dataset.scanner(columns=columns, filter="id = 'hit'",
            batch_size=1, limit=2).to_batches() for row in batch.to_pylist()]
    except (ValueError, OSError) as error:
        expected_error = type(error)
    else:
        expected_error = None
    actor, observed = await open_lookup(monkeypatch, dataset, columns=columns)
    try:
        if expected_error:
            with pytest.raises(expected_error):
                await actor({'id': 'hit'})
        else:
            assert (await actor({'id': 'hit'}))['matches'] == expected
        assert not observed.takes
    finally:
        await actor.aclose()


@pytest.mark.parametrize('count,maximum,success', [
    (1, 1, True), (2, 1, False), (64, 64, True), (65, 64, False)])
async def test_match_limit_checks_all_duplicates_without_truncation(
        tmp_path, monkeypatch, count, maximum, success):
    dataset = lance.write_dataset(pa.table({'id': ['hit']*count, 'seq': list(range(count))}),
                                  tmp_path/'rows', max_rows_per_file=16)
    actor, observed = await open_lookup(monkeypatch, dataset, max_matches=maximum)
    try:
        if success:
            assert (await actor({'id': 'hit'}))['matches'] == [
                {'id': 'hit', 'seq': i} for i in range(count)]
        else:
            with pytest.raises(ValueError, match='match/byte'):
                await actor({'id': 'hit'})
            assert not actor.cache
        assert all(len(ids) == 1 for ids in observed.takes)
    finally:
        await actor.aclose()
    assert observed.open_readers == 0


@pytest.mark.parametrize('indexed', [False, True])
async def test_arrow_and_aggregate_python_byte_bounds(tmp_path, monkeypatch, indexed):
    dataset = lance.write_dataset(pa.table({'id': ['wide', 'twice', 'twice'],
                                           'value': ['x'*4096, 'a', 'b']}), tmp_path/'rows')
    if indexed:
        dataset.create_scalar_index('id', 'BTREE')
    limit = row_size({'id': 'twice', 'value': 'a'}, 10000)*2 - 1
    actor, observed = await open_lookup(monkeypatch, dataset, max_matches=2, max_bytes=limit)
    try:
        with pytest.raises(ValueError, match='Arrow payload'):
            await actor({'id': 'wide'})
        with pytest.raises(ValueError, match='match/byte'):
            await actor({'id': 'twice'})
        assert not actor.cache and observed.open_readers == 0
    finally:
        await actor.aclose()


async def test_wide_unmatched_payloads_are_not_hydrated(tmp_path, monkeypatch):
    dataset = lance.write_dataset(pa.table({'id': ['huge', 'hit'],
        'value': ['x'*(2*1024**2), 'small']}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset, max_bytes=4096)
    try:
        assert (await actor({'id': 'hit'}))['matches'] == [{'id': 'hit', 'value': 'small'}]
        assert (await actor({'id': 'missing'}))['matches'] == []
        assert observed.takes == [[1]]
        assert all(scan['columns'] == [] for scan in observed.scans)
    finally:
        await actor.aclose()


async def test_wide_null_values_keep_one_row_arrow_bound(tmp_path, monkeypatch):
    # Fixed-size null children occupy Arrow buffers but become Python None.
    # Each row fits; collecting both payloads into one take would exceed it.
    values = pa.array([None, None], type=pa.list_(pa.int64(), 4096))
    dataset = lance.write_dataset(pa.table({'id': ['hit', 'hit'], 'value': values}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset, max_matches=2, max_bytes=40000)
    try:
        assert (await actor({'id': 'hit'}))['matches'] == [
            {'id': 'hit', 'value': None}, {'id': 'hit', 'value': None}]
        assert not observed.takes
    finally:
        await actor.aclose()


async def test_cache_stays_within_existing_key_and_byte_limits(tmp_path, monkeypatch):
    dataset = lance.write_dataset(pa.table({'id': list(range(140)),
        'value': ['x'*70000]*140}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset)
    try:
        for key in range(140):
            await actor({'id': key})
            assert len(actor.cache) <= 128 and actor.cache_bytes <= 8*1024**2
        assert len(observed.takes) == 140
        assert actor.cache_bytes > 7*1024**2
    finally:
        await actor.aclose()


async def test_payload_failure_closes_scan_and_keeps_failed_result_out_of_cache(tmp_path, monkeypatch):
    dataset = lance.write_dataset(pa.table({'id': ['hit']}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset)
    def fail():
        raise OSError('injected payload read failure')
    observed.before_take = fail
    try:
        with pytest.raises(OSError, match='injected'):
            await actor({'id': 'hit'})
        assert not actor.cache and observed.open_readers == 0
    finally:
        await actor.aclose()


async def test_cancellation_drains_payload_thread_before_close(tmp_path, monkeypatch):
    dataset = lance.write_dataset(pa.table({'id': ['hit']}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset)
    entered, release = threading.Event(), threading.Event()
    def block():
        entered.set()
        if not release.wait(5):
            raise RuntimeError('test did not release payload reader')
    observed.before_take = block
    task = asyncio.create_task(actor({'id': 'hit'}))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await actor.aclose()
    assert observed.open_readers == 0
    assert actor.table is None and not actor.cache


async def test_empty_fixed_table_has_no_payload_reads(tmp_path, monkeypatch):
    dataset = lance.write_dataset(pa.table({'id': pa.array([], type=pa.string()),
        'value': pa.array([], type=pa.string())}), tmp_path/'rows')
    actor, observed = await open_lookup(monkeypatch, dataset)
    try:
        assert (await actor({'id': 'missing'}))['matches'] == []
        assert (await actor({'id': 'missing'}))['matches'] == []
        assert len(observed.scans) == 1 and not observed.takes
    finally:
        await actor.aclose()
