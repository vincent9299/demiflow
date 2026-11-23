"""Durable model quota exhaustion keeps the original row-error semantics."""
import pytest

from demiflow import data
from demiflow.execution.run_journal import RunJournal
from demiflow.operator_llm.parser import parse_prompt_pack
from test_runtime_v3_native_execution import PACK, native_server
from _agent_runtime_fixtures import collect_before_commit, resume_collect


@pytest.mark.parametrize('ceiling', ['node', 'run'])
def test_model_quota_before_first_cut_and_resume_agree(tmp_path, native_server, monkeypatch, ceiling):
    from demiflow.execution.pipeline.run_context import RunContext
    pipeline = data.from_items([{'item': 'known'}, {'item': 'no-budget'}]).map_prompt_async(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'},
        output='answer', error_output='error', call_output='call', max_requests=1 if ceiling == 'node' else 2)
    if ceiling == 'run':
        original = RunContext.start.__func__
        monkeypatch.setattr(RunContext, 'start', classmethod(
            lambda cls, directory=None, **kwargs: original(cls, directory, **{**kwargs, 'limits': {'max_requests': 1}})))
    first, reference = collect_before_commit(pipeline, tmp_path / ceiling, monkeypatch)
    resumed = resume_collect(pipeline, reference)
    for rows in (first, resumed):
        assert rows[0]['answer'] == 'ok'
        assert rows[1]['error']['type'] == 'PromptBudgetExceededError'
        assert rows[1]['error']['detail'] == 'Operator LLM request budget exhausted'
        assert 'answer' not in rows[1]
    assert len(native_server) == 1
    with RunJournal(reference.directory)._connection() as db:
        assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
