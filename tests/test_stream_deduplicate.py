import asyncio
import pytest
from demiflow import data
from demiflow.data.plan import DeduplicateOp
from _worker_event_fixtures import emit, events


def test_deduplicate_releases_before_source_finishes_and_keeps_composite_identity(tmp_path):
    gate = tmp_path / 'delivered-a'
    seen = str(tmp_path / 'seen.jsonl')
    async def source(row):
        if row['id'] == 'b':
            async def delivered():
                while not gate.exists():
                    await asyncio.sleep(.001)
            await asyncio.wait_for(delivered(), 3)
        return row
    def consume(row):
        emit(seen, 'seen', value=row['id']); gate.touch(); return row
    rows = [{'id': 'a', 'concept': 'x', 'sha': 'same'},
            {'id': 'b', 'concept': 'x', 'sha': 'same'},
            {'id': 'c', 'concept': 'y', 'sha': 'same'}]
    ds = (data.from_items(rows).map_async(source, concurrency=1, queue_depth=1)
        .deduplicate(['concept', 'sha'], path=tmp_path/'keys', key='id'))
    assert any(isinstance(op, DeduplicateOp) for op in ds._plan.operations)
    ds.map(consume).run_stream()
    assert [e['value'] for e in events(seen)] == ['a', 'c']
    replay = (data.from_items(rows[::-1]).deduplicate(['concept', 'sha'], path=tmp_path/'keys', key='id')
        .take_all())
    assert [row['id'] for row in replay] == ['c', 'a']


def test_deduplicate_preserves_quota_free_journal_and_duplicate_receipts(tmp_path):
    path = tmp_path/'keys'
    rows = [{'id': 'a', 'sha': '1'}, {'id': 'b', 'sha': '1'}, {'skip': True}]
    options = dict(path=path, key='id', output='result', when=lambda r: not r.get('skip'))
    data.from_items(rows).admit_rows(unique_on=['sha'], **options).run_stream()
    seen = (data.from_items(rows[::-1]).deduplicate('sha', keep_duplicates=True, **options)
        .take_all())
    assert [r['result']['status'] for r in seen] == ['skipped', 'duplicate', 'admitted']


def test_deduplicate_failure_replay_and_capacity_are_explicit(tmp_path):
    path = tmp_path/'keys'
    def fail(row):
        raise RuntimeError('downstream failed')
    rows = [{'id': 'a', 'sha': '1'}, {'id': 'b', 'sha': '1'}]
    with pytest.raises(RuntimeError, match='downstream failed'):
        (data.from_items(rows).deduplicate('sha', path=path, key='id', max_entries=1)
            .map(fail).run_stream())
    seen = (data.from_items(rows[::-1]).deduplicate('sha', path=path, key='id', max_entries=1)
        .take_all())
    assert [r['id'] for r in seen] == ['a']
    with pytest.raises(ValueError, match='max_entries'):
        data.from_items([{'id': 'c', 'sha': '2'}]).deduplicate('sha', path=path, key='id', max_entries=1).run_stream()


@pytest.mark.parametrize('fields', [None, [], [''], ['x']*33, ['x'*257]])
def test_invalid_key_declarations_do_not_open_a_journal(tmp_path, fields):
    path = tmp_path/'keys'
    with pytest.raises(ValueError, match='deduplicate'):
        data.from_items([]).deduplicate(fields, path=path, key='id')
    assert not path.exists()
