"""Codex Prompt uses the original native journal, including replay and ceilings."""
import json

import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.operator_llm.client import create_async_operator_llm_client
from demiflow.operator_llm.parser import parse_prompt_pack
from test_codex_exec import PACK, fake_cli


def test_native_codex_response_and_replay_have_one_budget_authority(tmp_path, fake_cli):
    cli, capture = fake_cli
    pack = parse_prompt_pack(PACK)

    def pipeline():
        return data.from_items([{'task': 'one', 'images': []}]).map_prompt_async(
            'author', config=pack, inputs=['task', 'images'], output='answer',
            call_output='call', error_output='error', max_requests=1,
            options={'codex_exec': {'bin': str(cli)}, 'timeout_s': 10})

    class Interrupted(Exception):
        pass

    limits = {'memory_bytes': 2 * 1024**3, 'max_state_bytes': 16 * 1024**2}
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(workers=8):
            ref = run.ref
            rows = pipeline().take_all()
            assert rows[0]['answer'] == 7 and rows[0].get('error') is None
            assert rows[0]['call']['transport'] == 'codex_exec'
            with run.journal._connection() as db:
                assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
                assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
            raise Interrupted()
    with RunContext.resume(ref) as run, data.local_execution(workers=8):
        assert pipeline().take_all() == rows
        with run.journal._connection() as db:
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
    assert len(capture.read_text().splitlines()) == 1
    assert not list((tmp_path/'run').rglob('calls.sqlite'))


def test_codex_rejects_unverified_native_journal_before_starting_transport(fake_cli):
    cli, _ = fake_cli
    from demiflow.operator_llm.runtime import resolve_prompt
    model = resolve_prompt(parse_prompt_pack(PACK), 'author').model
    with pytest.raises(ValueError, match='bounded RemotePromptJournal'):
        create_async_operator_llm_client(model, {'codex_exec': {'bin': str(cli)}}, native_journal=object())
