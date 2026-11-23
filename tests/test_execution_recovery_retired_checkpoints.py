"""Legacy sidecars cannot select a second execution/recovery protocol."""
import asyncio
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution import RecoveryContractError


@pytest.mark.parametrize('entry', ['checkpoint', 'checkpoint_async', 'checkpoint_lance',
                                   'checkpoint_lance_async', 'native_lance'])
@pytest.mark.parametrize('cached', [False, True])
def test_retired_entry_rejects_before_input_files_or_worker(entry, cached, tmp_path, monkeypatch):
    calls = []
    def source():
        calls.append('source')
        yield {'value': 'new'}
    rows = data.from_iter(source)
    target = tmp_path / 'nested' / 'old.jsonl'
    if cached:
        target.parent.mkdir()
        target.write_text('{"value":"stale"}\n')
        target.with_suffix('.jsonl.meta.json').write_text('{"version":"v1"}')
    before = {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    def unexpected(*args, **kwargs):
        pytest.fail('retired entry dispatched a worker or inspected a Lance sidecar')
    monkeypatch.setattr(asyncio, 'to_thread', unexpected)
    import demiflow.lance.checkpoint as legacy_lance
    monkeypatch.setattr(legacy_lance, 'read_checkpoint_record', unexpected)
    with pytest.raises(RecoveryContractError, match='Explicit checkpoint orchestration is retired'):
        if entry == 'native_lance':
            legacy_lance.checkpoint_lance(rows, str(target), schema=None, fingerprint='v1')
        else:
            arguments = {'schema': None, 'fingerprint': 'v1'} if 'lance' in entry else {'version': 'v1'}
            result = getattr(rows, entry)(str(target), **arguments)
            if entry.endswith('_async'):
                asyncio.run(result)
    assert calls == []
    assert {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before
    assert target.parent.exists() == cached


@pytest.mark.parametrize('entry', ['checkpoint', 'checkpoint_async', 'checkpoint_lance', 'checkpoint_lance_async'])
def test_checked_flow_never_admits_retired_dataset_facades(entry, tmp_path):
    from demiflow.forge.platform import inspect_modules, LIMITS
    from test_forge_checkpoint import inspect
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    arguments = "schema=None, fingerprint='v1'" if 'lance' in entry else "version='v1'"
    body = ("from demiflow import data\ndef run(*, source, stage, target):\n"
            "    rows = data.read_lance(source, version=1)\n"
            f"    return rows.{entry}(stage, {arguments})\n")
    _, report, paths = inspect(tmp_path, catalog, body)
    assert report['status'] == 'invalid', report['diagnostics']
    assert any(entry in str(item) for item in report['diagnostics'])
    assert not Path(paths['stage']).exists()
    assert not Path(paths['target']).exists()


@pytest.mark.parametrize('entry', ['stream_checkpoint', 'operator_checkpoint', 'map_async', 'batch_map'])
@pytest.mark.parametrize('cached', [False, True])
def test_legacy_ledger_controls_cannot_allocate_or_recover(entry, cached, tmp_path):
    target = tmp_path / 'nested' / 'old-state'
    if cached:
        target.parent.mkdir()
        target.write_bytes(b'old-run-evidence-must-remain-untouched')
    async def convert(row):
        pytest.fail('retired checkpoint must reject before calling the operator')
    with pytest.raises(RecoveryContractError, match='Explicit checkpoint orchestration is retired'):
        if entry == 'stream_checkpoint':
            from demiflow import StreamCheckpoint
            StreamCheckpoint(target, identity={'source': 1})
        elif entry == 'operator_checkpoint':
            from demiflow.execution.operator_checkpoint import OperatorCheckpoint
            OperatorCheckpoint(target, operator='fixture')
        elif entry == 'map_async':
            data.from_items([]).map_async(convert, checkpoint=str(target))
        else:
            data.from_items([]).batch_map(convert, max_batch=1, checkpoint=str(target))
    assert target.parent.exists() == cached
    assert sorted(p.name for p in target.parent.iterdir()) == ['old-state'] if cached else not target.exists()
    if cached:
        assert target.read_bytes() == b'old-run-evidence-must-remain-untouched'


@pytest.mark.parametrize('entry', ['map_async', 'batch_map'])
@pytest.mark.parametrize('options', [{'checkpoint_operator': 'private'}, {'checkpoint_lease_s': 1}])
def test_retired_ledger_options_are_not_silently_ignored(entry, options):
    async def convert(row):
        return row
    if entry == 'batch_map':
        options = dict(options, max_batch=1)
    with pytest.raises(RecoveryContractError, match='Explicit checkpoint orchestration is retired'):
        getattr(data.from_items([]), entry)(convert, **options)
