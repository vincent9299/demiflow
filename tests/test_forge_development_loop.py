"""Engineering decisions and real subprocess feedback without a Workbench import."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

from demiflow.forge.review import review_project
from demiflow.forge.review_deepseek import extract_report
from demiflow.forge.schema import ForgeError


ROOT = Path(__file__).resolve().parents[1]


def response_for(prompt, *, blocker=False):
    body = json.loads(prompt.split('\n\n')[-1])
    checks = []
    for name, value in body['files'].items():
        if value['role'] != 'target':
            continue
        side = 'after' if value.get('after') else 'before'
        line = next(row for row in value[side].splitlines() if row.partition(' | ')[2].strip())
        number, _, quote = line.partition(' | ')
        checks.append(dict(topic='intent', path=name, side=side, line=int(number), quote=quote, reason='Checked goal.'))
    findings = []
    if blocker:
        findings.append(dict(checks[0], severity='blocking', trigger='fixture input', impact='incorrect value', suggestion='correct value'))
    return dict(provider='fixture', model='deterministic', session_id='review-session',
                report=dict(checks=checks, findings=findings, gaps=[]))


def test_twenty_eight_targets_all_receive_citations_and_one_candidate(tmp_path):
    (tmp_path/'operators').mkdir()
    paths = [f'operators/p{i}.py' for i in range(28)]
    for name in paths:
        (tmp_path/name).write_text('def run(row): return row\n')
    calls = []
    def reviewer(prompt):
        calls.append(prompt)
        return response_for(prompt)
    result = review_project(tmp_path, goal='Keep every source covered', paths=paths, reviewer=reviewer)
    assert result['status'] == 'passed' and result['complete'] and result['current']
    assert {row['path'] for row in result['checks']} == set(paths)
    assert len(calls) == 3
    assert result['budget']['calls'] == 34 - len(calls)


def test_malformed_finding_keeps_verified_blocker_and_cannot_pass(tmp_path):
    (tmp_path/'source.py').write_text('value = 1\n')
    calls = []
    def reviewer(prompt):
        if not calls:
            report = response_for(prompt, blocker=True)
            report['report']['findings'].append(dict(report['report']['findings'][0], severity='high'))
            calls.append(report)
        return calls[0]
    result = review_project(tmp_path, goal='Fix the value', paths=['source.py'], reviewer=reviewer)
    assert result['status'] == 'invalid' and not result['complete']
    assert len(result['findings']) == 1 and result['findings'][0]['severity'] == 'blocking'
    assert result['budget']['calls'] == 32 and result['diagnostics']


def test_shared_flow_dependency_findings_are_deduplicated_across_units(tmp_path):
    from demiflow.forge.project import initialize_project, register_flow
    root = tmp_path/'project'
    initialize_project(root, name='shared-review')
    (root/'flows').mkdir(exist_ok=True)
    (root/'operators').mkdir(exist_ok=True)
    shared = 'operators/shared.py'
    (root/shared).write_text('def normalize(row):\n    return dict(row, value=0)\n' + '# shared context\n'*2000)
    paths = [shared]
    for i in range(14):
        name = f'flows/flow_{i}.py'
        (root/name).write_text('from demiflow.data import from_items\nfrom operators.shared import normalize\ndef run():\n    return from_items([{\"id\": 1}]).map(normalize).materialize()\n')
        register_flow(root, name+':run')
        paths.append(name)
    repeated = []
    def reviewer(prompt):
        response = response_for(prompt)
        cited = next((check for check in response['report']['checks'] if check['path'] == shared), None)
        if cited:
            repeated.append(cited)
            response['report']['findings'] = [dict(cited, severity='blocking', trigger='input row',
                impact='Incorrect value', suggestion='Fix the shared mapping; review unit '+str(len(repeated)))]
        return response
    result = review_project(root, goal='All flows preserve the correct value', paths=paths, reviewer=reviewer)
    assert result['status'] == 'invalid' and result['complete'] and result['current']
    assert len(repeated) >= 2 and len(result['findings']) == 1
    assert set(paths) <= {check['path'] for check in result['checks']}


def test_explanatory_json_fence_is_unambiguous_and_multiple_objects_are_rejected():
    assert extract_report('评审结果如下：\r\n```json\r\n{"checks": []}\r\n```\n结束') == {'checks': []}
    with pytest.raises(ForgeError):
        extract_report('{"checks": []}\n{"checks": [1]}')
    with pytest.raises(ForgeError):
        extract_report('{"checks": [{"reason": "broken"}]')


def test_different_operation_directories_share_physical_writer_admission(tmp_path):
    from demiflow.forge.development_state import claim_writer, retire_writer, start_writer
    root = tmp_path/'project'; child = root/'nested'; child.mkdir(parents=True)
    roots = [tmp_path/'state-a', tmp_path/'state-b']
    handles = []
    key = None
    try:
        for directory in roots:
            directory.mkdir(mode=0o700)
            stream = (directory/'unit').open('xb')
            fcntl.flock(stream, fcntl.LOCK_SH)
            handles.append(stream)
        def claim(project, index):
            unit = roots[index]/'unit'; info = unit.stat()
            return claim_writer(project, str(roots[index]), unit, dict(device=info.st_dev, inode=info.st_ino))
        key = claim(root, 0); start_writer(key)
        for project in (root, child):
            with pytest.raises(ForgeError, match='active or unresolved'):
                claim(project, 1)
        handles[0].close()
        # An unlocked unit does not prove a completed author interval.
        with pytest.raises(ForgeError):
            claim(root, 1)
        assert not retire_writer(key, terminal=True, cleanup='unknown')
        assert retire_writer(key, terminal=True, cleanup='complete')
        key = claim(child, 1)
    finally:
        if key:
            retire_writer(key, terminal=True, cleanup='complete')
        for stream in handles:
            stream.close()


@pytest.fixture
def cli_project(tmp_path):
    from demiflow.forge.project import initialize_project, register_flow
    root = tmp_path/'project'
    initialize_project(root, name='loop-fixture')
    for role in ('flows', 'operators'):
        (root/role).mkdir(exist_ok=True)
    (root/'flows/main.py').write_text("from demiflow.data import from_items\ndef run():\n    return from_items([{'id': 1}]).materialize()\n")
    (root/'operators/value.py').write_text('def normalize(row):\n    """Normalize value.\n\n    Example:\n        {"id": 1} -> {"id": 1, "value": 2}\n    """\n    return dict(row, value=0)\n')
    register_flow(root, 'flows/main.py:run')
    author = tmp_path/'author'
    author.write_text('#!' + sys.executable + '\n' + textwrap.dedent('''
        import json, os, sys
        from pathlib import Path
        prompt = sys.stdin.read()
        resumed = 'resume' in sys.argv
        sid = '11111111-2222-3333-4444-555555555555'
        if resumed:
            assert sys.argv[sys.argv.index('resume')+1] == sid
            assert '独立评审阻断' in prompt
        with open(os.environ['FORGE_TEST_LOG'], 'a') as log:
            log.write(json.dumps(dict(session=sid, resumed=resumed, prompt=prompt))+'\\n')
        print(json.dumps(dict(type='thread.started', thread_id=sid)), flush=True)
        print(json.dumps(dict(type='item.completed', item=dict(type='error', message='startup diagnostic'))), flush=True)
        if os.environ.get('FORGE_TEST_UNCERTAIN'):
            raise SystemExit(2)
        print(json.dumps(dict(type='turn.started')), flush=True)
        value = 2 if resumed else 1
        Path('operators/value.py').write_text('def normalize(row):\\n    """Normalize value.\\n\\n    Example:\\n        {"id": 1} -> {"id": 1, "value": 2}\\n    """\\n    return dict(row, value='+str(value)+')\\n')
        print(json.dumps(dict(type='item.completed', item=dict(type='agent_message', text='done'))), flush=True)
        print(json.dumps(dict(type='turn.completed', usage={})), flush=True)
    '''))
    author.chmod(0o700)
    package = tmp_path/'reviewer'; (package/'lib').mkdir(parents=True)
    (package/'package.json').write_text(json.dumps({'name':'@deepseek-ai/dsh','version':'0.2.1-rc.2'}))
    (package/'lib/bin.js').write_text(textwrap.dedent('''
        import json, os, sys, uuid
        prompt = sys.stdin.read()
        data = json.loads(prompt.split('\\n\\n')[-1])
        checks, findings = [], []
        for path, source in data['files'].items():
            if source['role'] != 'target': continue
            side = 'after' if source.get('after') else 'before'
            lines = source[side].splitlines()
            number, _, quote = lines[0].partition(' | ')
            checks.append(dict(topic='intent', path=path, side=side, line=int(number), quote=quote, reason='Checked fixture goal'))
            for row in lines:
                if 'value=1)' in row:
                    number, _, quote = row.partition(' | ')
                    findings.append(dict(severity='blocking', topic='intent', path=path, side=side, line=int(number),
                        quote=quote, trigger='fixture input', impact='value must be 2', suggestion='correct the value'))
        for event in [dict(type='session', sessionId='session-'+str(uuid.uuid4()), cwd=os.getcwd()),
                      dict(type='status', phase='turn_end', reason=dict(kind='completed')),
                      dict(type='final', text=json.dumps(dict(checks=checks, findings=findings, gaps=[])))]:
            print(json.dumps(event), flush=True)
    '''))
    config = tmp_path/'review.json'
    config.write_text(json.dumps(dict(executable=str(package/'lib/bin.js'), node=sys.executable,
        model='fixture', version='0.2.1-rc.2', credential_env='FORGE_TEST_CREDENTIAL')))
    return root, author, config


def cli(root, state, author, config, *options, uncertain=False):
    # This guard applies before loading the CLI, including its full run path.
    code = '''import sys
sys.path.insert(0,sys.argv.pop(1))
class NoWorkbench:
    def find_spec(self,fullname,path=None,target=None):
        if fullname.startswith('demiflow.workbench'): raise AssertionError(fullname)
sys.meta_path.insert(0,NoWorkbench())
from demiflow.cli import main
main()
'''
    env = dict(os.environ, DEMIFLOW_FORGE_REVIEW_CONFIG=str(config), FORGE_TEST_CREDENTIAL='local-fixture',
               FORGE_TEST_LOG=str(state.parent/'calls.jsonl'), CODEX_HOME=str(state.parent/'native-home'))
    if uncertain:
        env['FORGE_TEST_UNCERTAIN'] = '1'
    return subprocess.run([sys.executable, '-I', '-B', '-c', code, str(ROOT), 'forge', 'develop',
        '--root', str(root), '--state-dir', str(state), '--executable', str(author), '--json', *options],
        env=env, capture_output=True, text=True, timeout=90)


def test_cli_drives_same_original_author_through_review_and_repair_without_workbench(cli_project, tmp_path):
    root, author, config = cli_project
    state = tmp_path/'state'
    result = cli(root, state, author, config, '--goal', 'Set value to 2 and preserve the flow')
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt['status'] == 'passed' and receipt['round'] == 1, receipt
    assert [r['review']['status'] for r in receipt['history']] == ['invalid', 'passed']
    calls = [json.loads(row) for row in (tmp_path/'calls.jsonl').read_text().splitlines()]
    assert len(calls) == 2 and calls[0]['session'] == calls[1]['session']
    assert calls[1]['resumed'] and 'value must be 2' in calls[1]['prompt']
    assert 'value=2)' in (root/'operators/value.py').read_text()
    status = cli(root, state, author, config, '--status')
    assert status.returncode == 0 and json.loads(status.stdout)['id'] == receipt['id']
    resumed = cli(root, state, author, config, '--resume')
    assert resumed.returncode == 0
    assert len((tmp_path/'calls.jsonl').read_text().splitlines()) == 2


def test_cli_never_replays_a_sent_but_unacknowledged_prompt(cli_project, tmp_path):
    root, author, config = cli_project
    state = tmp_path/'state'
    result = cli(root, state, author, config, '--goal', 'Do the work', uncertain=True)
    assert result.returncode == 3, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt['phase'] == 'unknown' and receipt['delivery'] == 'unknown'
    assert receipt['author']['session'] and not receipt['author']['acknowledged']
    resumed = cli(root, state, author, config, '--resume')
    assert resumed.returncode == 3
    assert len((tmp_path/'calls.jsonl').read_text().splitlines()) == 1


def test_guard_keeps_native_forge_registration_then_restores_last_authorized_version(cli_project, tmp_path, monkeypatch):
    from demiflow.forge.project import register_flow
    from demiflow.forge.project_guard import (CONTROL_ENV, capture_protected_files,
        prepare_control_writes, apply_control_writes, enforce_protected_files)
    root, _, _ = cli_project
    (root/'flows/second.py').write_bytes((root/'flows/main.py').read_bytes())
    snapshot = capture_protected_files(root)
    evidence = tmp_path/'controls.json'
    prepare_control_writes(root, evidence, 'interval')
    monkeypatch.setenv(CONTROL_ENV, str(evidence))
    register_flow(root, 'flows/second.py:run')
    authorized = (root/'workspace.json').read_bytes()
    assert b'flows/second.py:run' in authorized
    (root/'workspace.json').write_text('{"name":"unauthorized"}')
    apply_control_writes(root, evidence, 'interval', snapshot)
    assert enforce_protected_files(root, snapshot) == ['workspace.json']
    assert (root/'workspace.json').read_bytes() == authorized


def test_manual_verified_blocker_carries_scope_to_recheck_even_with_auto_review_off(tmp_path):
    from demiflow.forge.development import assess_turn
    source = tmp_path/'source.py'; source.write_text('value = 1\n')
    author = dict(provider='codex', session='original', completed=True, acknowledged=True, cleanup='complete')
    calls = []
    def reviewer(prompt):
        calls.append(prompt)
        return response_for(prompt, blocker=True)
    arguments = dict(author=author, goal='Set value to 2', changes=[dict(path='source.py')],
                     reviewer=reviewer, inspect=False, auto_review=False)
    first = assess_turn(tmp_path, selection={}, **arguments)
    assert first['status'] == 'invalid' and first['feedback']['options']['selection'] == {}
    next_options = first['feedback']['options']
    arguments.pop('auto_review')
    second = assess_turn(tmp_path, **next_options, **arguments)
    assert second['status'] == 'invalid' and second['feedback'] is None
    assert len(calls) == 2 and second['budget']['calls'] == 32


def test_guard_only_cli_repair_keeps_static_check_policy(cli_project, tmp_path):
    from demiflow.forge.development import run_development
    from demiflow.forge.development_state import DevelopmentState
    root, _, _ = cli_project
    calls = []
    def author(root, prompt, **options):
        calls.append(prompt)
        options['notify'](dict(session='same-author', delivery='acknowledged'))
        if len(calls) == 1:
            (root/'workspace.json').write_text('{"name":"unauthorized"}')
        return dict(provider='codex', session='same-author', completed=True, acknowledged=True, cleanup='complete')
    def reviewer(*args, **kwargs):
        pytest.fail('A guard-only repair must not introduce a paid review')
    state = DevelopmentState(tmp_path/'guard-state', root)
    result = run_development(state, goal='Preserve the project configuration', binding=dict(provider='codex'),
        author=author, reviewer=reviewer, auto_review=True)
    assert len(calls) == 2 and result['status'] == 'checked'
    assert 'demiflow forge add flow' in calls[1] and 'manage_project_layout' not in calls[1]
    assert all(item['options']['selection'] is None and item['review'] is None for item in result['history'])


def test_repair_allowance_is_shared_across_review_invocations(tmp_path):
    (tmp_path/'source.py').write_text('value = 1\n')
    budget = dict(calls=34, seconds=1800)
    calls = []
    def reviewer(prompt):
        calls.append(prompt)
        return dict(provider='fixture', model='fixture', session_id='fixture', report={})
    for _ in range(2):
        result = review_project(tmp_path, goal='Inspect', paths=['source.py'], reviewer=reviewer, budget=budget)
        assert result['status'] == 'incomplete'
    assert len(calls) == 3 and budget['repairs'] == 0


def test_large_preimage_is_kept_independently_of_ui_preview(tmp_path):
    from demiflow.forge.source_snapshot import capture, read_source
    root = tmp_path/'project'; root.mkdir()
    raw = '# line\n' * 3000
    (root/'source.py').write_text(raw)
    facts = capture(root, ['source.py'], source_dir=tmp_path/'sources')
    assert 'preview' not in facts['source.py']
    assert read_source(tmp_path/'sources', facts['source.py']) == raw


def test_renamed_active_root_cannot_admit_a_nested_author(tmp_path, monkeypatch):
    from demiflow.forge import development_state as state
    domain = tmp_path/'domain'; domain.mkdir(mode=0o700)
    monkeypatch.setattr(state, 'writer_domain', lambda:domain)
    root = tmp_path/'parent'; (root/'child').mkdir(parents=True)
    unit = tmp_path/'unit'; unit.touch(mode=0o600)
    info = unit.stat(); identity = dict(device=info.st_dev, inode=info.st_ino)
    key = state.claim_writer(root, 'first', unit, identity)
    state.start_writer(key)
    moved = tmp_path/'moved'; root.rename(moved)
    try:
        with pytest.raises(ForgeError, match='identity changed'):
            state.claim_writer(moved/'child', 'second', unit, identity)
    finally:
        state.retire_writer(key, terminal=True, cleanup='complete')
