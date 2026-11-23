"""Original native journal backpressure survives authenticated worker RPC."""
from demiflow import data
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.execution.contracts.errors import _CapacityPending
from test_runtime_v3_native_execution import PACK, native_server
from test_runtime_v3_native_rpc_replay import interrupted_run, service, event


def test_native_capacity_retry_preserves_exact_type():
    error = restore_failure(failure_payload(_CapacityPending()))
    assert type(error) is _CapacityPending


def test_native_capacity_refusal_is_not_memoized_or_charged(tmp_path, monkeypatch):
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, _):
        journal = authority._native_journal()
        def full(request):
            raise _CapacityPending()
        request = event(run, store)
        with monkeypatch.context() as patch:
            patch.setattr(journal, 'reserve', full)
            try:
                authority.dispatch(request)
            except _CapacityPending:
                pass
            else:
                raise AssertionError('The original capacity refusal must reach the RPC encoder')
        assert request.operation_id not in authority._native_reserve_replies
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 0
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone() is None
        # The exact same command can be admitted when capacity becomes free.
        reply = authority.dispatch(request)
        assert reply.payload['value'] is True
        assert authority.dispatch(request) == reply
        with run.journal._connection() as db:
            assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1


def test_real_eight_concurrent_prompts_wait_for_original_response_capacity(tmp_path, native_server, monkeypatch):
    from demiflow.execution.journal.prompt import RunPromptJournal
    import time
    from types import SimpleNamespace
    native_server.barrier = SimpleNamespace(wait=lambda timeout: time.sleep(.05))
    pending, runs = [], []
    apply = RunPromptJournal._apply_operation
    def observe(journal, db, item, effects):
        if not runs:
            runs.append(journal.run)
        try:
            return apply(journal, db, item, effects)
        except _CapacityPending:
            # Observe the real pre-admission branch, do not manufacture it.
            pending.append(item.payload[0])
            assert db.execute('SELECT 1 FROM calls WHERE request_key=?', (item.payload[0],)).fetchone() is None
            raise
    monkeypatch.setattr(RunPromptJournal, '_apply_operation', observe)
    # Keep the original test's 64 MiB control budget and 8 MiB+64 KiB response
    # bound: eight concurrent calls cannot all reserve responses at once.
    from demiflow.data.api import DataAPI
    saved = DataAPI().from_items([{'item': str(i)} for i in range(8)]).map_prompt_async('enrich',
            config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer', concurrency=8,
            max_requests=8, options={'trust_env': False}).materialize()
    try:
        run = runs[0]
        assert run.limits['max_control_bytes'] == 64*1024**2
        assert run.directory.is_relative_to('/tmp')
        assert len(saved.take_all()) == len(native_server) == 8
        assert all(row['answer'] == 'ok' for row in saved.take_all())
        assert pending
        with run.journal._connection() as db:
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 8
            assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 8
            assert int(db.execute("SELECT value FROM journal_state WHERE name='native_response_reserved'").fetchone()[0]) == 0
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    finally:
        saved.release()
