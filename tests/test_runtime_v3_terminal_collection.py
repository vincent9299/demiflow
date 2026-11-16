"""Real public terminal windows, with worker-owned lazy input and bounded returns."""
import json
import math
import os
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext


MEMORY = 192 * 1024


def _source(root, count, width=6000):
    log = str(root / 'source.jsonl')
    closed = str(root / 'source.closed')

    def rows():
        try:
            for i in range(count):
                with open(log, 'a') as stream:
                    stream.write(json.dumps({'i': i, 'pid': os.getpid()}) + '\n')
                yield {'i': i, 'payload': 'x' * width, 'typed': (b'\xff', -0.0, None)}
        finally:
            Path(closed).write_text('closed')
    return rows


def _events(root):
    return [json.loads(line) for line in (root / 'source.jsonl').read_text().splitlines()]


def test_public_take_all_rejects_accumulation_over_driver_budget(tmp_path):
    with pytest.raises(DataflowError, match='collected bytes') as error:
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
            workers=2, worker_mode='process', memory_bytes=MEMORY,
        ) as local:
            data.from_iter(_source(tmp_path, 48)).take_all()
    assert error.value.code == 'BUDGET_EXCEEDED'
    assert local.stats['engine'] == 'dataflow_v3'
    assert local.stats['admitted_bounds']['max_resident_bytes'] == MEMORY
    assert all(event['pid'] != os.getpid() for event in _events(tmp_path))
    assert (tmp_path / 'source.closed').exists()


def test_public_batch_rejects_before_reading_whole_requested_batch(tmp_path):
    with pytest.raises(DataflowError, match='batch.*byte') as error:
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
            workers=2, worker_mode='thread', memory_bytes=MEMORY,
        ):
            list(data.from_iter(_source(tmp_path, 128)).iter_batches(
                batch_size=128, batch_format='pandas'))
    assert error.value.code == 'BUDGET_EXCEEDED'
    assert 0 < len(_events(tmp_path)) < 128
    assert (tmp_path / 'source.closed').exists()


def test_public_take_all_within_budget_preserves_typed_rows(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
        workers=2, worker_mode='process', memory_bytes=MEMORY,
    ) as local:
        result = data.from_iter(_source(tmp_path, 12)).take_all()
        assert local.stats['engine'] == 'dataflow_v3'
    assert [row['i'] for row in result] == list(range(12))
    assert all(row['typed'] == (b'\xff', -0.0, None) for row in result)
    assert all(type(row['typed']) is tuple and math.copysign(1, row['typed'][1]) < 0 for row in result)
    assert (tmp_path / 'source.closed').exists()


def test_take_all_exact_output_row_limit_is_not_charged_for_probe(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30, 'max_output_rows': 3}), data.local_execution(
        workers=2, worker_mode='thread', memory_bytes=MEMORY,
    ):
        result = data.from_iter(_source(tmp_path, 3, width=10)).take_all(limit=3)
    assert [row['i'] for row in result] == [0, 1, 2]


def test_take_all_zero_limit_empty_input_succeeds(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
        workers=2, worker_mode='thread', memory_bytes=MEMORY,
    ):
        assert data.from_iter(_source(tmp_path, 0)).take_all(limit=0) == []
    assert (tmp_path / 'source.closed').exists()


@pytest.mark.parametrize('worker_mode', ['thread', 'process'])
@pytest.mark.parametrize('limit', [0, 2, 65])
def test_direct_collection_stops_input_at_first_overflow(tmp_path, worker_mode, limit):
    from demiflow.execution.run_journal import RunJournal
    with pytest.raises(ValueError, match='more than the take_all limit'):
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30,
                'checkpoint_rows': 8}), data.local_execution(workers=2, worker_mode=worker_mode):
            data.from_iter(_source(tmp_path, limit + 10, width=10)).take_all(limit=limit)
    assert [event['i'] for event in _events(tmp_path)] == list(range(limit + 1))
    assert (tmp_path / 'source.closed').exists()
    journal = RunJournal(tmp_path / 'run')
    assert journal.execution_action('action-1')['result'] is None
    assert not any(c['prepared']['sinks']['take_all']['manifest']['final']
                   for c in journal.execution_commits('action-1'))


def test_collection_output_limit_does_not_limit_filtered_input(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
            workers=2, worker_mode='process'):
        result = data.from_iter(_source(tmp_path, 6, width=10)).filter(
            lambda row: row['i'] >= 4).take_all(limit=2)
    assert [row['i'] for row in result] == [4, 5]
    assert [event['i'] for event in _events(tmp_path)] == list(range(6))
    assert (tmp_path / 'source.closed').exists()


@pytest.mark.parametrize('limit,count', [(0, 1), (2, 3), (3, 4)])
def test_take_all_limit_rejects_overflow_without_truncation(tmp_path, limit, count):
    with pytest.raises(ValueError, match='take_all limit'):
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30, 'max_output_rows': 3}), data.local_execution(
            workers=2, worker_mode='thread', memory_bytes=MEMORY,
        ):
            data.from_iter(_source(tmp_path, count, width=10)).take_all(limit=limit)


@pytest.mark.parametrize('drop_last,lengths', [(False, [3, 3, 1]), (True, [3, 3])])
def test_explicit_batch_size_and_tail_are_preserved(tmp_path, drop_last, lengths):
    with RunContext.start(tmp_path / 'run', limits={'timeout_s': 30}), data.local_execution(
        workers=2, worker_mode='thread', memory_bytes=MEMORY,
    ):
        batches = list(data.from_iter(_source(tmp_path, 7, width=10)).iter_batches(
            batch_size=3, batch_format='pandas', drop_last=drop_last))
    assert [len(batch) for batch in batches] == lengths
    assert [i for batch in batches for i in batch['i'].tolist()] == list(range(sum(lengths)))


@pytest.mark.parametrize('mode', ['collect_bytes', 'collect_rows', 'batch_bytes', 'overflow_probe'])
def test_admission_precedes_second_record_decode(monkeypatch, mode):
    """Narrow sequencing proof complements the real worker cases above."""
    from dataclasses import replace
    from types import SimpleNamespace
    from demiflow.execution import dataflow_terminals as terminals
    from demiflow.execution.contracts.dataflow import Record
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    from demiflow.planning.dataflow_policy import resolve_runtime_policy

    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    row = {'typed': (b'\xff', -0.0, None)}
    size = len(_encode_payload(row, max_bytes=4096))
    updates = {'max_resident_bytes': size} if mode == 'collect_bytes' else (
        {'max_output_rows': 1} if mode == 'collect_rows' else (
        {'max_batch_bytes': size} if mode == 'batch_bytes' else {}))
    bounds = replace(bounds, **updates)
    state = {'decoded': 0, 'closed': False, 'cleanup': False}

    def records():
        try:
            for i in range(2):
                yield Record(str(i) * 64, {'index': i}, None, row, size, i)
        finally:
            state['closed'] = True

    handle = SimpleNamespace(bounds=bounds, services=SimpleNamespace(bounds=bounds, store=None), rows=records,
        _cleanup=lambda: state.update(cleanup=True))

    def decode(record, unused):
        state['decoded'] += 1
        assert state['decoded'] == 1, 'second record decoded before budget/limit admission'
        return row

    monkeypatch.setattr(terminals, '_record_row', decode)
    expected = ValueError if mode == 'overflow_probe' else DataflowError
    with pytest.raises(expected):
        if mode == 'batch_bytes':
            list(terminals.iterate_batches(handle, batch_size=20, batch_format='list', drop_last=False))
        else:
            terminals.collect_rows(handle, limit=1 if mode == 'overflow_probe' else None,
                bounds=bounds, verify_complete=mode == 'overflow_probe')
    assert state == {'decoded': 1, 'closed': True, 'cleanup': True}
