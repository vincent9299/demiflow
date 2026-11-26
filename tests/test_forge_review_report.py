import json

from click.testing import CliRunner
import pytest

from demiflow.cli import main
from demiflow.forge.review import review_project
from demiflow.forge.schema import ForgeError


@pytest.fixture
def project(tmp_path):
    module = tmp_path / 'operators' / 'normalize.py'
    module.parent.mkdir()
    module.write_text('def normalize(row):\n    return row["name"].lower()\n')
    return tmp_path


def response():
    reference = dict(topic='intent', path='operators/normalize.py', line=2, side='after',
                     quote='return row["name"].lower()')
    return dict(provider='deepseek', model='fixture', session_id='fixture-session',
                report=dict(checks=[dict(reference, reason='The implementation lowercases names.')],
                            findings=[], gaps=[]))


def invoke(project, payload):
    return review_project(project, paths=('operators/normalize.py',), goal='Normalize names',
                          reviewer=lambda prompt: payload)


def test_each_review_has_its_own_identity_and_bound_evidence(project):
    outcome = invoke(project, response())
    assert outcome['status'] == 'passed' and outcome['reviewer']['session_id'] == 'fixture-session'
    assert outcome['checks'][0]['line'] == 2
    assert outcome['review_id'] != invoke(project, response())['review_id']


def test_review_must_cite_source_at_the_claimed_location(project):
    cases = (
        ({'line': 1}, '源码不匹配'),
        ({'line': 99}, '行号越界'),
        ({'line': True}, 'line 必须为整数'),
        ({'path': '../elsewhere'}, 'path 不在本次评审材料中'),
        ({'side': 'before'}, '缺少 operators/normalize.py 的 before 版本源码'),
        ({'quote': 'return row["name"].upper()'}, '源码不匹配'),
    )
    for updates, expected in cases:
        candidate = response()
        candidate['report']['checks'][0].update(updates)
        outcome = invoke(project, candidate)
        assert outcome['status'] == 'incomplete' and outcome['diagnostics']
        detail = outcome['diagnostics'][0]['detail']
        assert '独立评审报告缺少必要字段或源码依据' in detail
        assert expected in detail, (updates, detail)


def test_incomplete_review_includes_specific_reason_and_draft_summary(project):
    candidate = response()
    candidate['report']['findings'] = [dict(
        topic='wiring', path='operators/normalize.py', line=2, side='after',
        quote='wrong quote', severity='blocking',
        trigger='missing key', impact='KeyError on name', suggestion='use row.get("name")',
    )]
    outcome = invoke(project, candidate)
    assert outcome['status'] == 'incomplete'
    details = [row['detail'] for row in outcome['diagnostics']]
    assert any('findings[1]' in d and '源码不匹配' in d for d in details), details
    assert any('原始输出摘要' in d and 'KeyError on name' in d for d in details), details

    empty_checks = response()
    empty_checks['report']['checks'] = []
    outcome_empty = invoke(project, empty_checks)
    assert outcome_empty['status'] == 'incomplete'
    assert 'checks 为空' in outcome_empty['diagnostics'][0]['detail']


def test_blocker_optional_suggestion_and_gap_have_distinct_effects(project):
    candidate = response()
    reference = candidate['report']['checks'][0]
    candidate['report']['findings'] = [dict(reference, severity='suggestion', trigger='when input is missing',
                                            impact='the sample is skipped', suggestion='include a sample')]
    assert invoke(project, candidate)['status'] == 'passed'
    candidate['report']['gaps'].append('Missing business output contract')
    assert invoke(project, candidate)['status'] == 'incomplete'
    candidate['report']['findings'][0]['severity'] = 'blocking'
    assert invoke(project, candidate)['status'] == 'invalid'


def test_inflight_changes_cannot_receive_a_current_report(project):
    def reviewer(prompt):
        assert '2 | ' in prompt and 'Normalize names' in prompt
        (project / 'operators' / 'normalize.py').write_text('def normalize(row): return row\n')
        return response()
    outcome = review_project(project, goal='Normalize names', paths=['operators/normalize.py'], reviewer=reviewer)
    assert outcome['status'] == 'incomplete' and outcome['gaps']


def test_model_unavailable_and_fabricated_pass_are_incomplete(project):
    def unavailable(prompt):
        raise ForgeError('Provider unavailable', code='inspection_incomplete')
    outcome = review_project(project, goal='Check', paths=['operators/normalize.py'], reviewer=unavailable)
    assert outcome['status'] == 'incomplete' and outcome['diagnostics'][0]['detail'] == 'Provider unavailable'
    assert invoke(project, {'status': 'passed'})['status'] == 'incomplete'


def test_multiple_deliverables_need_individual_source_evidence(project):
    (project / 'operators' / 'extra.py').write_text('def extra(): return 1\n')
    result = review_project(project, paths=('operators/normalize.py', 'operators/extra.py'), goal='Check both',
                            reviewer=lambda p: response())
    assert result['status'] == 'incomplete' and 'extra.py' in str(result['gaps'])


def test_bounded_followup_reads_checked_project_evidence(project):
    folder = project / 'tests'
    folder.mkdir()
    (folder / 'unusual_contract.py').write_text('expected = "lowercase"\n')
    calls = []

    def reviewer(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return dict(response(), report={'request_files': ['tests/unusual_contract.py']})
        return response()

    result = review_project(project, goal='Normalize names', paths=['operators/normalize.py'], reviewer=reviewer)
    assert result['status'] == 'passed'
    assert len(calls) == 2 and 'lowercase' in calls[1]
    assert 'tests/unusual_contract.py' in result['additional_read']['paths']

    for path in ['../outside.py', 'configs/credentials.json', 'tests/absent.py']:
        def denied(prompt):
            return dict(response(), report={'request_files': [path]})
        result = review_project(project, goal='Check', paths=['operators/normalize.py'], reviewer=denied)
        assert result['status'] == 'incomplete' and result['diagnostics']


def test_additional_read_counts_workflow_registration_in_unit_budget(project, monkeypatch):
    from demiflow.forge import review_evidence
    workflow = project / 'workflows' / 'more.json'
    workflow.parent.mkdir()
    workflow.write_text('{"nodes":[]}')
    target = 'operators/normalize.py'
    monkeypatch.setattr(review_evidence, 'UNIT_BYTES', (project / target).stat().st_size + workflow.stat().st_size)
    calls = []

    def reviewer(prompt):
        calls.append(prompt)
        return dict(response(), report={'request_files': ['workflows/more.json']})

    report = review_project(project, goal='Review the helper', paths=[target], reviewer=reviewer)
    assert len(calls) == 1
    assert report['status'] == 'incomplete' and report['diagnostics'][0]['code'] == 'resource_limit'


def test_cli_json_and_human_output(project, monkeypatch):
    from demiflow.forge import review_deepseek
    monkeypatch.setattr(review_deepseek, 'run_reviewer', lambda prompt, **options: response())
    cli = CliRunner()
    args = ['forge', 'review', '--root', str(project), '--goal', 'Normalize names',
            '--path', 'operators/normalize.py']
    human = cli.invoke(main, args)
    assert human.exit_code == 0 and 'operators/normalize.py:2' in human.output
    result = cli.invoke(main, [*args, '--json'])
    assert result.exit_code == 0 and json.loads(result.output)['status'] == 'passed'
    bad = cli.invoke(main, [*args, '--entry', 'module.py:run', '--json'])
    assert bad.exit_code == 2 and json.loads(bad.output)['status'] == 'failed'
