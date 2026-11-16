"""Lance I/O behavior: managed writes, pinned reads, checkpoints, reconciliation."""
from demiflow.lance.control import control_directory, table_lock_path
import asyncio
import fcntl
import json
from pathlib import Path

from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.lance.registry import write_registered_table

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("lance")

from demiflow.data.api import DataAPI
from demiflow.errors import (
    InvalidLanceRequest, LanceExecutionError, LanceWriteConflict,
    LanceWriteError, LanceResourceNotFound,
)


def ds(rows):
    return DataAPI().from_items(rows)


def write_direct(uri, rows, schema=None):
    import lance
    table = pa.Table.from_pylist(rows, schema=schema)
    return lance.write_dataset(table, uri, mode="overwrite")


# ---------------------------------------------------------------- existing write/read behavior

def test_write_lance_create_append_roundtrip(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}, {"k": "b"}]).write_lance(uri)
    ds([{"k": "c"}]).write_lance(uri)
    import lance
    assert lance.dataset(uri).count_rows() == 3


def test_write_lance_rejects_zero_rows(tmp_path):
    with pytest.raises(InvalidLanceRequest):
        ds([]).write_lance(str(tmp_path / "empty.lance"))


def test_write_lance_append_schema_mismatch_is_indeterminate(tmp_path):
    # Keep the historical selector. A deterministic schema mismatch must now
    # fail in native prepare; it is not evidence of uncertain publication.
    import lance
    from demiflow.execution import RunContext
    from demiflow.execution.run_journal import RunJournal
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    version = lance.dataset(uri).version
    with pytest.raises(InvalidLanceRequest, match="Lance append schema differs at field 'k'"):
        with RunContext.start(tmp_path / 'mismatch-run'):
            ds([{"other": 1}]).write_lance(uri)
    assert lance.dataset(uri).version == version
    assert lance.dataset(uri).to_table().to_pylist() == [{"k": "a"}]
    journal = RunJournal(tmp_path / 'mismatch-run')
    for fact in journal.execution_commits('action-1'):
        if fact['status'] == 'pending':
            assert fact['prepared'] is None and fact['receipts'] == {}
            continue
        assert fact['status'] == 'acked'
        assert not fact['prepared']['sinks']['write_lance']['manifest']['final']
        assert fact['receipts']['write_lance']['data'].get('native_receipt') is None


def test_read_lance_pinned_version_ignores_later_appends(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    api = DataAPI()
    import lance
    pinned_version = lance.dataset(uri).version
    pinned = api.read_lance(uri, version=pinned_version)
    ds([{"k": "b"}]).write_lance(uri)
    assert sorted(r["k"] for r in pinned.take_all()) == ["a"]
    assert lance.dataset(uri).count_rows() == 2


def test_write_lance_cas_conflict(tmp_path):
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    import lance
    stale = lance.dataset(uri).version
    ds([{"k": "b"}]).write_lance(uri)
    with pytest.raises(LanceWriteConflict):
        ds([{"k": "c"}]).write_lance(uri, expected_version=stale)
    assert lance.dataset(uri).count_rows() == 2


def test_receipt_roundtrip(tmp_path):
    from demiflow.lance.model import LanceWriteReceipt
    uri = str(tmp_path / "t.lance")
    ds([{"k": "a"}]).write_lance(uri)
    import lance
    version = lance.dataset(uri).version
    receipt = LanceWriteReceipt(
        request_hash="sha256:x", uri=uri, expected_version=None,
        committed_version=version, input_rows=1, written_rows=1,
        schema_hash="sha256:y", status="committed",
    )
    clone = LanceWriteReceipt.from_dict(json.loads(json.dumps(receipt.to_dict())))
    assert clone == receipt


# ---------------------------------------------------------------- historical checkpoint intent, current native protocol

SCHEMA = pa.schema([("k", pa.string()), ("n", pa.int64())])


def checkpoint_uri(tmp_path, name="out"):
    return str(tmp_path / f"{name}.lance")


class _Interrupted(Exception):
    pass


def _write_pinned(dataset, uri, *, schema=SCHEMA):
    """Test-only composition of the public terminal and its fixed-version read."""
    receipt = dataset.write_lance(uri, mode='create', schema=schema, return_receipt=True)
    return DataAPI().read_lance(uri, version=receipt.committed_version)


def _registered(root, factory, *, fingerprint='fp-1', schema=SCHEMA):
    return write_registered_table(root, 'out.lance', schema=schema,
        schema_name='checkpoint-io', schema_version='1', rows_factory=factory,
        fingerprint=fingerprint)


def _registration_journal(root):
    directory = control_directory(str(Path(root)/'out.lance'))
    pointer = json.loads((directory/'registered-native.json').read_text())
    return RunJournal(directory/pointer['directory'] if pointer['relative'] else pointer['directory'])


def test_checkpoint_writes_pinned_replayable_snapshot(tmp_path):
    uri = checkpoint_uri(tmp_path)
    saved = _write_pinned(ds([{'k':'a','n':1},{'k':'b','n':2}]), uri)
    assert sorted(r['k'] for r in saved.take_all()) == ['a','b']
    import lance
    version = lance.dataset(uri).version
    write_direct(uri, [{'k':'z','n':9}], schema=SCHEMA)
    assert sorted(r['k'] for r in saved.take_all()) == ['a','b']
    assert DataAPI().read_lance(uri, version=version).take_all()[0]['k'] == 'a'


def test_checkpoint_replay_skips_execution(tmp_path):
    uri = checkpoint_uri(tmp_path)
    calls = tmp_path/'calls'
    def observe(row):
        with calls.open('a') as stream: stream.write(row['k']+'\n')
        return row
    def execute():
        return _write_pinned(ds([{'k':'a','n':1}]).map(observe), uri)
    with pytest.raises(_Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            execute()
            raise _Interrupted('after original ACK')
    original = RunJournal(reference.directory).execution_commits('action-1')
    with RunContext.resume(reference):
        replayed = execute()
    assert calls.read_text().splitlines() == ['a']
    assert RunJournal(reference.directory).execution_commits('action-1') == original
    assert [r['k'] for r in replayed.take_all()] == ['a']


def test_checkpoint_rejects_changed_fingerprint(tmp_path):
    _registered(tmp_path, lambda: iter([{'k':'a','n':1}]))
    original = _registration_journal(tmp_path).execution_commits('action-1')
    def forbidden():
        raise AssertionError('a mismatched fingerprint must reject before input')
        yield
    with pytest.raises(ValueError, match='fingerprint|request differs'):
        _registered(tmp_path, forbidden, fingerprint='fp-2')
    assert _registration_journal(tmp_path).execution_commits('action-1') == original


def test_checkpoint_zero_rows_commits_valid_empty_table(tmp_path):
    uri = checkpoint_uri(tmp_path)
    def execute(): return _write_pinned(ds([]), uri)
    with pytest.raises(_Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            execute()
            raise _Interrupted('after original empty publication')
    import lance
    version = lance.dataset(uri).version
    with RunContext.resume(reference):
        replayed = execute()
    assert replayed.take_all() == []
    assert lance.dataset(uri).count_rows() == 0
    assert lance.dataset(uri).version == version


def test_checkpoint_enforces_explicit_schema(tmp_path):
    uri = checkpoint_uri(tmp_path)
    with pytest.raises(ValueError, match='rogue'):
        _write_pinned(ds([{'k':'a','n':1,'rogue':True}]), uri)
    assert not Path(uri).exists()
    with pytest.raises(ValueError, match='not-an-int|convert|int64'):
        _write_pinned(ds([{'k':'a','n':'not-an-int'}]), uri)
    assert not Path(uri).exists()


def test_checkpoint_nullable_fields_accept_missing(tmp_path):
    schema = pa.schema([('k',pa.string()),('note',pa.string())])
    saved = _write_pinned(ds([{'k':'a'}]), checkpoint_uri(tmp_path), schema=schema)
    assert saved.take_all() == [{'k':'a','note':None}]


def test_checkpoint_rejects_concurrent_writer(tmp_path):
    # Registration's public writer admission uses its actual bounded lock.
    control = control_directory(checkpoint_uri(tmp_path))
    control.mkdir(parents=True, exist_ok=True)
    def forbidden():
        raise AssertionError('a concurrent writer must reject before input')
        yield
    with (control/'registered-write.lock').open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(LanceWriteConflict, match='registered writer'):
            _registered(tmp_path, forbidden)
    assert not Path(checkpoint_uri(tmp_path)).exists()


def test_checkpoint_retry_after_partial_attempt(tmp_path):
    # Missing original ownership is not permission to delete arbitrary bytes.
    uri = checkpoint_uri(tmp_path)
    Path(uri).mkdir()
    foreign = Path(uri)/'junk.bin'
    foreign.write_bytes(b'partial attempt without original ownership')
    with pytest.raises((LanceExecutionError, LanceWriteConflict)):
        _write_pinned(ds([{'k':'a','n':1}]), uri)
    assert foreign.read_bytes() == b'partial attempt without original ownership'


def test_checkpoint_never_overwrites_plain_written_table(tmp_path):
    import lance
    uri = checkpoint_uri(tmp_path)
    write_direct(uri, [{'k':'original','n':1}], schema=SCHEMA)
    version = lance.dataset(uri).version
    with pytest.raises(LanceWriteConflict, match='exists'):
        _write_pinned(ds([{'k':'replacement','n':2}]), uri)
    assert lance.dataset(uri).to_table().column('k').to_pylist() == ['original']
    assert lance.dataset(uri).version == version


def test_checkpoint_recovers_pending_publish(tmp_path, monkeypatch):
    # Interrupt the original protocol after native publication, before its
    # runtime receipt/ACK. Never fabricate an old pending sidecar.
    import lance
    from demiflow.execution import commit
    calls = tmp_path/'calls'
    def factory():
        with calls.open('a') as stream: stream.write('source\n')
        yield {'k':'a','n':1}
    fired = []
    def interrupt(name, commit_id):
        if name == 'after_sink_commit_before_receipt' and Path(checkpoint_uri(tmp_path)).exists():
            fired.append(commit_id)
            raise _Interrupted('after native publication before original receipt')
    with monkeypatch.context() as patch:
        patch.setattr(commit, 'fault_point', interrupt)
        with pytest.raises(_Interrupted, match='after native publication'):
            _registered(tmp_path, factory)
    journal = _registration_journal(tmp_path)
    original_id = journal.recovery_identity()['run_id']
    before = journal.execution_commits('action-1')
    assert len(fired) == 1 and before[-1]['status'] == 'prepared'
    assert not before[-1]['receipts']
    version = lance.dataset(checkpoint_uri(tmp_path)).version
    ref, rows, replayed = _registered(tmp_path, factory)
    assert ref.lance_version == version and rows == 1
    assert journal.recovery_identity()['run_id'] == original_id
    after = journal.execution_commits('action-1')
    assert len(after) == len(before) and all(f['status']=='acked' for f in after)
    assert [f['commit_id'] for f in after] == [f['commit_id'] for f in before]
    assert calls.read_text().splitlines() == ['source']
    assert DataAPI().read_lance(ref.resolve(tmp_path),version=ref.lance_version).take_all() == [{'k':'a','n':1}]
    assert not list(control_directory(checkpoint_uri(tmp_path)).glob('pending-*'))


def test_checkpoint_failed_attempt_leaves_target_untouched(tmp_path):
    uri = checkpoint_uri(tmp_path)
    with pytest.raises(ValueError, match='rogue_field'):
        with RunContext.start(tmp_path/'run') as run:
            _write_pinned(ds([{'k':'b','n':2},{'rogue_field':True}]), uri)
    assert not Path(uri).exists()
    assert not list(run.directory.glob('materializations/*/lance/*.arrow'))
    facts = run.journal.execution_commits('action-1')
    assert not any(f['receipts'].get('write_lance',{}).get('data',{}).get('native_receipt') for f in facts)


def test_checkpoint_replay_binds_caller_uri_after_move(tmp_path):
    import shutil
    first = tmp_path/'root1'; first.mkdir()
    ref, _, _ = _registered(first, lambda: iter([{'k':'a','n':1}]))
    journal = _registration_journal(first)
    original = journal.execution_commits('action-1')
    native = original[-1]['receipts']['write_lance']['data']['native_receipt']['data']
    original_uri = native['uri']
    def forbidden():
        raise AssertionError('confirmed relocated output must never reopen source')
        yield
    previous = first
    for name in ('root2','root3'):
        current = tmp_path/name
        shutil.move(str(previous), str(current))
        assert not previous.exists()
        replay, rows, reused = _registered(current, forbidden)
        assert reused and rows == 1 and replay.lance_version == ref.lance_version
        assert DataAPI().read_lance(replay.resolve(current),version=replay.lance_version).take_all() == [{'k':'a','n':1}]
        assert _registration_journal(current).execution_commits('action-1') == original
        assert original_uri == str(first/'out.lance')
        previous = current


def test_checkpoint_replay_detects_missing_table(tmp_path):
    import shutil
    _registered(tmp_path, lambda: iter([{'k':'a','n':1}]))
    original = _registration_journal(tmp_path).execution_commits('action-1')
    shutil.rmtree(checkpoint_uri(tmp_path))  # fixture's own published destination
    def forbidden():
        raise AssertionError('a missing confirmed destination must not re-execute')
        yield
    with pytest.raises(LanceResourceNotFound):
        _registered(tmp_path, forbidden)
    assert not Path(checkpoint_uri(tmp_path)).exists()
    assert _registration_journal(tmp_path).execution_commits('action-1') == original


def test_checkpoint_rejects_remote_uri(tmp_path, monkeypatch):
    # The local registration API must reject URI roots before invoking input.
    # Keep cwd private even if a regression misinterprets the URI as a path.
    monkeypatch.chdir(tmp_path)
    def forbidden():
        raise AssertionError('remote registration must reject before input')
        yield
    with pytest.raises((InvalidLanceRequest, ValueError), match='local|URI|scheme|filesystem'):
        _registered('s3://bucket', forbidden)
    assert not (tmp_path/'s3:').exists()


def test_checkpoint_async(tmp_path):
    uri = checkpoint_uri(tmp_path)
    async def main():
        await asyncio.sleep(0)
        return await asyncio.to_thread(_write_pinned, ds([{'k':'a','n':1}]), uri)
    saved = asyncio.run(main())
    assert [r['k'] for r in saved.take_all()] == ['a']


def test_checkpoint_chainable_after_save(tmp_path):
    saved = _write_pinned(ds([{'k':'a','n':1},{'k':'b','n':2}]), checkpoint_uri(tmp_path))
    doubled = saved.map(lambda row: {'k':row['k'],'n':row['n']*10})
    assert sorted(r['n'] for r in doubled.take_all()) == [10,20]


# ---------------------------------------------------------------- reconciliation

def test_find_committed_append_by_marker(tmp_path):
    import lance
    from demiflow.lance.reconcile import find_committed_append
    from demiflow.lance.write import (
        commit_lance_append, prepare_lance_append, write_lance_fragment,
    )
    from demiflow.lance.model import LanceWriteSpec

    uri = str(tmp_path / "cas.lance")
    write_direct(uri, [{"k": "a"}])
    version = lance.dataset(uri).version

    spec = LanceWriteSpec(uri=uri, expected_version=version)
    prepared = prepare_lance_append(spec)
    fragment = write_lance_fragment(
        prepared, task_index=0, batches=iter([pa.table({"k": ["b", "c"]})]),
    )
    receipt = commit_lance_append(prepared, (fragment,))
    assert receipt.status == "committed"

    found = find_committed_append(uri, operation_id=prepared.operation_id)
    assert found == receipt.committed_version
    assert find_committed_append(uri, operation_id="no-such-operation") is None


def test_reconcile_resolves_indeterminate_receipt(tmp_path):
    import lance
    from demiflow.lance.reconcile import reconcile_lance_append
    from demiflow.lance.write import (
        commit_lance_append, prepare_lance_append, write_lance_fragment,
    )
    from demiflow.lance.model import LanceWriteReceipt, LanceWriteSpec

    uri = str(tmp_path / "cas.lance")
    write_direct(uri, [{"k": "a"}])
    version = lance.dataset(uri).version
    spec = LanceWriteSpec(uri=uri, expected_version=version)
    prepared = prepare_lance_append(spec)
    fragment = write_lance_fragment(
        prepared, task_index=0, batches=iter([pa.table({"k": ["b"]})]),
    )
    receipt = commit_lance_append(prepared, (fragment,))
    assert receipt.status == "committed"

    # simulate an indeterminate copy of the same commit (commit succeeded,
    # caller never learned the outcome)
    from demiflow._compat.error_transport import make_error
    indeterminate = LanceWriteReceipt(
        request_hash=receipt.request_hash, uri=receipt.uri,
        expected_version=receipt.expected_version,
        committed_version=receipt.committed_version,
        input_rows=receipt.input_rows, written_rows=None,
        schema_hash=receipt.schema_hash, status="indeterminate",
        error=make_error(
            module="test", type_name="ConnectionReset", message="reset",
        ),
    )
    resolved = reconcile_lance_append(prepared, (fragment,), indeterminate)
    assert resolved.status == "committed"
    assert resolved.committed_version == receipt.committed_version

    unrelated = LanceWriteReceipt(
        request_hash="sha256:other", uri=uri,
        expected_version=version + 100,
        committed_version=None,
        input_rows=0, written_rows=None,
        schema_hash=receipt.schema_hash, status="indeterminate",
        error=make_error(
            module="test", type_name="Timeout", message="timed out",
        ),
    )
    assert reconcile_lance_append(prepared, (fragment,), unrelated).status == "indeterminate"


def test_checkpoint_supports_async_operator_plan(tmp_path):
    uri = checkpoint_uri(tmp_path)
    calls = tmp_path/'async-calls'
    async def double(row):
        with calls.open('a') as stream: stream.write(row['k']+'\n')
        await asyncio.sleep(0)
        return {**row,'n':row['n']*2}
    def execute():
        return _write_pinned(ds([{'k':'a','n':1},{'k':'b','n':2}]).map_async(double), uri)
    with pytest.raises(_Interrupted):
        with RunContext.start(tmp_path/'run') as run:
            reference = run.ref
            execute()
            raise _Interrupted('after async plan publication')
    with RunContext.resume(reference):
        replayed = execute()
    assert sorted(r['n'] for r in replayed.take_all()) == [2,4]
    assert sorted(calls.read_text().splitlines()) == ['a','b']


def test_checkpoint_async_from_running_loop(tmp_path):
    import lance
    uri = checkpoint_uri(tmp_path,'loop')
    async def main():
        ticks = []
        async def tick():
            await asyncio.sleep(0)
            ticks.append('caller-loop-live')
        task = asyncio.create_task(tick())
        saved = await asyncio.to_thread(_write_pinned, ds([{'k':'a','n':7}]), uri)
        await task
        assert ticks == ['caller-loop-live']
        return saved
    saved = asyncio.run(main())
    assert saved.take_all()[0]['n'] == 7
    assert lance.dataset(uri).count_rows() == 1
