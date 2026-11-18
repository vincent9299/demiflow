"""Real local protocol, fake LLM streams: no model, credential or business call."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.development.managed import ManagedTasks, ReviewAuthority
from demiflow.forge.development.project import ForgeError
from demiflow.workbench.conversations import review
from demiflow.workbench.conversations.policy import Limits
from test_forge_development_managed import managed, read_all, claims
from test_forge_development_migration import fixture


def ready(api):
    read_all(api)
    assert api.check('session')['gates']['check'] == 'passed'
    current = api.verify('session')
    assert current['gates']['verify'] == 'passed'
    return current


def setup_review(api, monkeypatch, tmp_path, *, mode='pass', mutate=None):
    node = shutil.which('node')
    if not node: pytest.skip('Node is required for the pinned native-service protocol fixture')
    value = claims()
    if mode == 'fail':
        value['DF-04'] = dict(status='fail', reason='Fixture operator does not preserve the required row', evidence=['orders/rules.py'])
    output = json.dumps(dict(claims=value)) if mode != 'malformed' else '{"claims":"passed"}'
    plugin = Path(review.__file__).with_name('deepseek_review.cjs')
    driver = tmp_path / 'fake-native-llm.cjs'
    driver.write_text('''
const plugin=require(process.argv[2]);
const mode=process.argv[3], text=process.argv[4];
const ctx={effect(){},llm:{async *stream(options){
  if(options.tools.length || options.sessionId || options.messages.length!==1 || options.provider!=='fixture' || options.model!=='fixture-model' || options.maxTokens!==16384 || options.reasoningEffort!=='low')
    throw new Error('Not an independent no-tools request');
  const input=options.messages[0].content[0].text;
  if(!input.includes('original_goal') || !input.includes('review_rules') || !input.includes('verification')) throw new Error('Missing evidence');
  if(mode==='tool') {yield {type:'tool-call-delta',index:0,id:'bad',name:'shell',argumentsDelta:'{}'};return;}
  let index=0;
  if(mode==='reasoning' || mode==='reasoning_truncated') {
    yield {type:'block-start',index,blockType:'reasoning'};
    yield {type:'reasoning-delta',index,text:'bounded fixture reasoning'};
    yield {type:'block-end',index,block:{type:'reasoning',text:'bounded fixture reasoning'}};
    if(mode==='reasoning_truncated') {
      // Exact event shape observed from the real account route: all output
      // tokens spent on reasoning, no text block, then usage and max-tokens.
      yield {type:'usage',usage:{inputTokens:217,outputTokens:8192,cacheReadTokens:15232,totalTokens:23641}};
      yield {type:'finish',reason:{kind:'max-tokens'}};return;
    }
    index++;
  }
  yield {type:'block-start',index,blockType:'text'};
  yield {type:'text-delta',index,text};
  yield {type:'block-end',index,block:{type:'text',text}};
  yield {type:'usage',usage:{inputTokens:20,outputTokens:200,totalTokens:220}};
  yield {type:'finish',reason:{kind:mode==='truncated'?'max-tokens':'stop'}};
}}};
plugin.apply(ctx,{provider:'fixture',model:'fixture-model',reasoningEffort:'low',timeoutMs:5000,maxOutputBytes:65536});
''')
    monkeypatch.setattr(review, 'review_capabilities', lambda *_: dict(available=True, reason='offline fixture', mode='model_only'))

    def launch(config, home, root):
        assert config['_review'] is True and config['_review_timeout_ms'] <= 180000
        assert config['effort'] == 'low'
        assert str(home) == root and Path(home) != api.project_root
        assert not (Path(home) / 'developer-session').exists()
        if mutate: mutate()
        return [node, str(driver), str(plugin), mode, output], dict(PATH='/usr/bin:/bin', HOME=str(home), LANG='C.UTF-8')
    monkeypatch.setattr(review.DeepSeek, 'launch', launch)
    deployment = SimpleNamespace(state=tmp_path/'state', limits=Limits(), providers={'deepseek':dict(model='fixture-model',model_provider='fixture',effort='high')})
    session = dict(id='session', provider='deepseek', provider_session='developer-native-session')
    return deployment, session


def test_native_no_tools_review_receipt_survives_restart_and_binds_input(managed, monkeypatch, tmp_path):
    api, project, _ = managed
    before = (project.root/'orders/rules.py').read_text()
    (project.root/'orders/rules.py').write_text(before+'# independently reviewed candidate\n')
    api.refresh_context('session', 'fixture changed')
    current = ready(api)
    deployment, session = setup_review(api, monkeypatch, tmp_path)
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == 'acknowledged' and not result['retryable'], result
    assert result['projection']['gates']['review'] == 'passed', result
    assert api.evaluate_acceptance('session')['development_state'] == 'accepted'
    restarted = ManagedTasks(tmp_path/'state','fixture',project.root,review_authority=review.review_authority(tmp_path/'state'))
    assert restarted.review_authority.available is False
    assert restarted.status('session')['development_state'] == 'accepted'
    key = tmp_path/'state/forge-development/.review-authority.key'
    assert key.stat().st_mode & 0o777 == 0o600
    record = json.loads((api.store.directory('session')/'task.json').read_text())
    artifact = api.store.directory('session') / next(iter(record['managed']['review_inputs'].values()))
    evidence = json.loads(artifact.read_text())
    assert evidence['original_goal'] == 'Develop fixture'
    assert '# independently reviewed candidate' in evidence['differences'][0]['diff']
    assert evidence['verification']['verification_source'] == 'live_tree_rechecked'
    evidence['original_goal'] = 'forged intent'
    artifact.write_text(json.dumps(evidence))
    assert restarted.status('session')['gates']['review'] == 'failed'
    assert not list((tmp_path/'state/forge-development').glob('review-*'))


def test_review_failure_is_validated_feedback_and_cannot_be_flipped_green(managed, monkeypatch, tmp_path):
    api, _, _ = managed
    current = ready(api)
    deployment, session = setup_review(api, monkeypatch, tmp_path, mode='fail')
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == 'acknowledged' and result['retryable'] is True, result
    assert result['projection']['gates']['review'] == 'failed'
    assert result['feedback'][0]['rule_id'] == 'DF-04'
    path = api.store.directory('session')/'task.json'
    state = json.loads(path.read_text()); state['review']['passed'] = True
    path.write_text(json.dumps(state))
    assert api.status('session')['gates']['review'] == 'failed'


@pytest.mark.parametrize('mode,dispatch', [('tool','unknown'),('truncated','acknowledged'),('malformed','acknowledged')])
def test_unsafe_or_invalid_review_never_receives_a_receipt(managed, monkeypatch, tmp_path, mode, dispatch):
    api, _, _ = managed
    current = ready(api)
    deployment, session = setup_review(api, monkeypatch, tmp_path, mode=mode)
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == dispatch and not result['retryable'], result
    assert result['projection']['gates']['review'] in {'unavailable','unknown'}
    state = json.loads((api.store.directory('session')/'task.json').read_text())
    assert 'receipt' not in state['review']


def test_candidate_change_during_review_cannot_receive_receipt(managed, monkeypatch, tmp_path):
    api, project, _ = managed
    current = ready(api)
    def mutate():
        with (project.root/'orders/rules.py').open('a') as stream: stream.write('# changed during review\n')
    deployment, session = setup_review(api, monkeypatch, tmp_path, mutate=mutate)
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == 'acknowledged' and not result['retryable']
    assert result['projection']['gates']['check'] == 'stale'
    state = json.loads((api.store.directory('session')/'task.json').read_text())
    assert state['review'] is None


def test_missing_safe_route_and_deadline_are_not_dispatched(managed, monkeypatch, tmp_path):
    api, _, _ = managed
    current = ready(api)
    deployment = SimpleNamespace(state=tmp_path/'state',providers={'codex':dict(version='0.164.0',model='fixture')})
    session = dict(id='session',provider='codex')
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == 'not_sent' and result['projection']['gates']['review'] == 'unavailable'
    monkeypatch.setattr(review,'review_capabilities',lambda *_:dict(available=True))
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()-1)
    assert result['dispatch'] == 'not_sent'


def test_capabilities_never_discover_codex_auth(monkeypatch):
    monkeypatch.setenv('EXPLICIT_REVIEW_KEY','fixture')
    config=dict(version='0.164.0',model='fixture')
    assert not review.review_capabilities('codex',config)['available']
    config.update(base_url='https://example.invalid/v1',credential_env='EXPLICIT_REVIEW_KEY')
    assert review.review_capabilities('codex',config)['available']
    config['base_url']='http://example.invalid/v1'
    assert not review.review_capabilities('codex',config)['available']


def test_original_baseline_cannot_be_recaptured_or_skipped(managed):
    api, project, _ = managed
    current=ready(api)
    state_path=api.store.directory('session')/'task.json'
    state=json.loads(state_path.read_text())
    baseline=state['managed']['bound_contract']['review_baseline']
    (api.store.directory('session')/baseline['artifact']).write_text('{}')
    with pytest.raises(ForgeError,match='baseline evidence changed'):
        api.review_material('session',current['source_fingerprint'])


def test_review_attempt_budget_is_per_turn_without_replacing_original_task(managed):
    api, _, _=managed
    original=api.status('session')['baseline_fingerprint']
    bundles=[]
    for turn in range(3):
        api.note('session','decision',f'Actual user follow-up {turn}',event_id=f'turn-{turn}')
        current=ready(api)
        bundle=api.review_material('session',current['source_fingerprint'])
        bundles.append(bundle['input_digest'])
        assert bundle['input']['original_goal']=='Develop fixture'
        assert 'fixture' in bundle['input']['verification']['commands'][0]['log_tail']
        assert api.status('session')['baseline_fingerprint']==original
    assert len(set(bundles))==3


def test_persistent_authority_rejects_public_key_mode(tmp_path):
    authority=review.review_authority(tmp_path/'state')
    key=tmp_path/'state/forge-development/.review-authority.key'
    key.chmod(0o644)
    with pytest.raises(ForgeError,match='0600'):
        review.review_authority(tmp_path/'state')


def test_enforced_acceptance_requires_both_actual_phase_receipts_and_current_backend(managed):
    api, project, authority = managed
    api.prepare_task('enforced','orders','Isolated fixture',['orders'],['No production'],
        contract=dict(allow_tests=True,max_verification_seconds=5,developer_session_id='developer',require_enforcement=True))
    read_all(api,'enforced')
    current=api.check('enforced'); fingerprint=current['source_fingerprint']
    with pytest.raises(ForgeError,match='external execution boundary'):
        api.verify('enforced')

    def launch(argv, *, cwd, env, scratch, timeout):
        # Trusted-launch fixture only. Real kernel isolation is tested by B1.
        process=subprocess.Popen(argv,cwd=cwd,env=env,stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        process.execution_assurance='enforced'
        process.execution_evidence=dict(backend='fixture-backend',implementation='fixture-v1',execution_id='fixture-exec')
        return process
    verified=api.verify('enforced',launch=launch)
    assert verified['gates']['verify']=='passed'
    assert verified['execution_evidence']==dict(developer='missing',verifier='passed')
    assert verified['assurance']=='gate_only' and not verified['ready']
    value=claims()
    receipt=authority.issue('enforced',fingerprint,value,provider='fixture',model='fixture',session_id='reviewer',mode='no-tools')
    with pytest.raises(ForgeError,match='authority domain'):
        api.record_execution('enforced',fingerprint,receipt=receipt)
    api.record_review('enforced',fingerprint,value,receipt=receipt)
    assert not api.evaluate_acceptance('enforced')['ready']
    execution=authority.issue_execution('enforced',fingerprint,phase='developer',backend='fixture-backend',implementation='fixture-v1',execution_id='developer-exec')
    api.record_execution('enforced',fingerprint,receipt=execution)
    accepted=api.evaluate_acceptance('enforced')
    assert accepted['development_state']=='accepted' and accepted['assurance']=='enforced'
    authority.execution_identity=dict(backend='fixture-backend',implementation='fixture-v2')
    stale=api.status('enforced')
    assert not stale['ready'] and stale['assurance']=='gate_only'
    assert stale['execution_evidence']==dict(developer='stale',verifier='stale')
    with pytest.raises(ForgeError,match='current trusted implementation'):
        authority.issue_execution('enforced',fingerprint,phase='developer',backend='fixture-backend',implementation='fixture-v1',execution_id='old-exec')


def test_verifier_stderr_is_bounded_real_failure_feedback(managed):
    api, project, authority=managed
    # Configure fixed fixture before preparing another independent task.
    config=json.loads(project.path.read_text())
    config['pipelines']['orders']['verify'][0]['argv'][-1]='import sys; sys.stderr.write("fixture failure: missing row\\n"); sys.exit(2)'
    project.path.write_text(json.dumps(config))
    api.prepare_task('failure','orders','Fixture failure',['orders'],['No production'],contract=dict(allow_tests=True,max_verification_seconds=5,developer_session_id='developer'))
    read_all(api,'failure'); api.check('failure')
    def launch(argv, *, cwd, env, scratch, timeout):
        return subprocess.Popen(argv,cwd=cwd,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    result=api.verify('failure',launch=launch)
    assert result['gates']['verify']=='failed'
    assert 'returncode=2' in result['feedback'][0]['reason']
    assert 'fixture failure: missing row' in result['feedback'][0]['reason']
    assert result['feedback'][0]['allowed_next_action']=='fix_within_scope'
    assert result['repairable'] is True
    assert len(json.dumps(result['feedback']).encode())<=12*1024


def test_timed_out_verifier_is_uncertain_and_acceptance_checks_deadline(managed, monkeypatch):
    api, project, authority=managed
    config=json.loads(project.path.read_text())
    config['pipelines']['orders']['verify'][0].update(argv=[sys.executable,'-c','import time; time.sleep(3)'],timeout=1)
    project.path.write_text(json.dumps(config))
    api.prepare_task('timeout','orders','Fixture timeout',['orders'],['No production'],contract=dict(
        allow_tests=True,max_verification_seconds=1,developer_session_id='developer'))
    read_all(api,'timeout'); api.check('timeout')
    result=api.verify('timeout',timeout_seconds=.1)
    assert result['gates']['verify']=='failed'
    assert result['feedback'][0]['allowed_next_action']=='inspect_uncertain_execution'
    assert result['repairable'] is False
    with pytest.raises(ForgeError,match='deadline exhausted'):
        api.evaluate_acceptance('timeout',deadline=time.time()-1)


def test_acceptance_deadline_rechecked_after_candidate_scan(managed, monkeypatch):
    from test_forge_development_managed import pass_gates
    api, _, authority=managed
    pass_gates(api,authority)
    moment=time.time(); deadline=moment+10
    original=api._projection
    def slow_projection(*args,**kwargs):
        result=original(*args,**kwargs)
        monkeypatch.setattr('demiflow.forge.development.managed.time.time',lambda:deadline+1)
        return result
    monkeypatch.setattr(api,'_projection',slow_projection)
    with pytest.raises(ForgeError,match='deadline exhausted'):
        api.evaluate_acceptance('session',deadline=deadline)
    state=json.loads((api.store.directory('session')/'task.json').read_text())
    assert state['managed']['acceptance'] is None


def test_actual_sandbox_timeout_exit_code_cannot_authorize_verifier_replay(managed, tmp_path):
    from demiflow.forge.development.runner import run
    from demiflow.forge.development.managed import _feedback
    from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox
    from demiflow.workbench.conversations.development import runtime_paths
    _, project, _=managed
    def launch(argv, *, cwd, env, scratch, timeout):
        return launch_sandbox(argv,spec=SandboxSpec(read_paths=(str(project.root),),write_paths=(),
            runtime_paths=runtime_paths(dict(executable=sys.executable)),protected_paths=(str(project.root),),
            private_home=str(scratch),role='verifier',seconds=.1),env=env,cwd=cwd)
    outcome=run(dict(id='timeout',argv=[sys.executable,'-I','-B','-c','import time; time.sleep(3)'],env={},timeout=5),
        project,tmp_path/'short-sandbox',launch=launch)
    assert outcome['returncode']==124 and outcome['failure']=='sandbox_execution_incomplete'
    assert _feedback(dict(verification=dict(fingerprint='candidate', commands=[outcome])),tmp_path,'candidate')[0]['allowed_next_action']=='inspect_uncertain_execution'


def test_real_confined_developer_and_verifier_then_no_tools_review_accepts(tmp_path, monkeypatch):
    from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox
    from demiflow.workbench.conversations.development import verification_launcher, runtime_paths
    root=tmp_path/'project'; root.mkdir(); project=fixture(root)
    config=json.loads(project.path.read_text())
    # An owned fixture verifies the real boundary, including sibling authority denial.
    key=tmp_path/'state/forge-development/.review-authority.key'
    code='''
import os
from pathlib import Path
for path in [Path('orders/rules.py'), Path(%r)]:
    try:
        with path.open('ab') as stream: stream.write(b'forbidden')
    except PermissionError: pass
    else: raise AssertionError('verifier escaped readonly scope')
Path(os.environ['HOME'],'fixture-result').write_text('scratch only')
assert '# confined developer' in Path('orders/rules.py').read_text()
print('real verifier confined')
''' % str(key)
    config['pipelines']['orders']['verify'][0]['argv']=[sys.executable,'-I','-B','-c',code]
    project.path.write_text(json.dumps(config))
    authority=review.review_authority(tmp_path/'state')
    api=ManagedTasks(tmp_path/'state','fixture',root,review_authority=authority)
    api.prepare_task('session','orders','Implement fixture',['orders'],['No production'],contract=dict(
        allow_tests=True,max_verification_seconds=5,developer_session_id='developer',require_enforcement=True))
    home=tmp_path/'developer-home'; home.mkdir(mode=0o700)
    runtime=runtime_paths(dict(executable=sys.executable))
    process=launch_sandbox([sys.executable,'-I','-B','-c',
        'from pathlib import Path; p=Path("orders/rules.py"); p.write_text(p.read_text()+"# confined developer\\n")'],
        spec=SandboxSpec(read_paths=(str(root),),write_paths=(str(root/'orders'),),runtime_paths=runtime,
            protected_paths=(str(root/'AGENTS.md'),),denied_paths=(str(tmp_path/'state'),),private_home=str(home),seconds=5),
        env=dict(PATH='/usr/bin:/bin',LANG='C.UTF-8'),cwd=root)
    _, stderr=process.communicate(timeout=8)
    assert process.returncode==0,stderr.decode()
    assert process.execution_assurance=='enforced'
    api.refresh_context('session','Actual confined developer candidate'); read_all(api)
    fingerprint=api.status('session')['source_fingerprint']
    receipt=authority.issue_execution('session',fingerprint,phase='developer',**process.execution_evidence)
    api.record_execution('session',fingerprint,receipt=receipt)
    api.check('session')
    deployment=SimpleNamespace(state=tmp_path/'state',providers={'deepseek':dict(executable=sys.executable)},limits=Limits(),value={})
    session=dict(id='session',workspace='fixture',provider='deepseek')
    current=api.verify('session',launch=verification_launcher(deployment,session,dict(root=str(root))))
    assert current['gates']['verify']=='passed',current
    assert current['execution_evidence']==dict(developer='passed',verifier='passed')
    deployment, session=setup_review(api,monkeypatch,tmp_path)
    result=review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['projection']['ready'],result
    accepted=api.evaluate_acceptance('session')
    assert accepted['development_state']=='accepted' and accepted['assurance']=='enforced'
    restarted=ManagedTasks(tmp_path/'state','fixture',root,review_authority=review.review_authority(tmp_path/'state'))
    assert restarted.status('session')['assurance']=='enforced'


@pytest.mark.parametrize('output_type,status,allowed', [('message','completed',True),('function_call','completed',False),('message','incomplete',False)])
def test_explicit_responses_route_has_no_tools_or_auth_discovery(monkeypatch, output_type, status, allowed):
    import io
    monkeypatch.setenv('EXPLICIT_REVIEW_KEY','fixture-key')
    received=[]
    response=dict(status=status,model='fixture',output=[dict(type=output_type,role='assistant',status='completed',
        content=[dict(type='output_text',text=json.dumps(dict(claims=claims())))])])
    class Response(io.BytesIO): status=200
    class Opener:
        def open(self,request,timeout):
            received.append(request)
            assert request.full_url=='https://example.invalid/v1/responses'
            payload=json.loads(request.data)
            assert payload['tools']==[] and payload['tool_choice']=='none' and payload['store'] is False
            assert 'previous_response_id' not in payload and 'conversation' not in payload
            return Response(json.dumps(response).encode())
    monkeypatch.setattr(review,'build_opener',lambda *args:Opener())
    config=dict(base_url='https://example.invalid/v1',credential_env='EXPLICIT_REVIEW_KEY',model='fixture')
    if allowed:
        assert json.loads(review._responses_request(config,'Review fixture',time.time()+5))['claims']==claims()
    else:
        with pytest.raises(review.ReviewUnavailable): review._responses_request(config,'Review fixture',time.time()+5)
    assert len(received)==1


@pytest.fixture
def readonly_data(tmp_path):
    """Actual owned Lance bytes, never a business store or model request."""
    import lance
    import pyarrow as pa
    root = tmp_path/'project'; root.mkdir()
    project = fixture(root)
    (root/'data').mkdir()
    lance.write_dataset(pa.table({'id':[1, 2], 'value':['one', 'two']}), str(root/'data/source.lance'))
    (root/'data/large.bin').write_bytes(b'\x00\xff' * (128*1024))
    (root/'parameters').mkdir()
    (root/'parameters/input.json').write_text('{"dataset":"data/source.lance","version":1}')
    (root/'workspace.json').write_text('{"fixture":"readonly configuration"}')
    config = json.loads(project.path.read_text())
    config['readonly'] = ['data', 'parameters', 'workspace.json']
    project.path.write_text(json.dumps(config))
    authority = ReviewAuthority()
    api = ManagedTasks(tmp_path/'state', 'fixture', root, review_authority=authority)
    api.prepare_task('session','orders','Review an owned binary input fixture',['orders'],['No production'],
        contract=dict(allow_tests=True,max_verification_seconds=5,developer_session_id='developer'))
    return api, project


def test_readonly_lance_is_hash_bound_opaque_evidence_and_review_can_complete(readonly_data, monkeypatch, tmp_path):
    import hashlib
    api, project = readonly_data
    source = project.root/'orders/rules.py'
    source.write_text(source.read_text() + '# candidate change still shown in full\n')
    api.refresh_context('session', 'Candidate changed')
    current = ready(api)
    bundle = api.review_material('session', current['source_fingerprint'])
    material = bundle['input']
    opaque = {name:value for name,value in material['sources'].items() if value.get('kind') == 'readonly_artifact'}
    assert any(name.startswith('data/source.lance/') for name in opaque)
    assert 'data/large.bin' in opaque
    for name, item in opaque.items():
        raw = (project.root/name).read_bytes()
        assert item['sha256'] == hashlib.sha256(raw).hexdigest() and item['bytes'] == len(raw)
        assert item['content'] == 'not_included' and 'text' not in item
        assert item['basis'] == 'unchanged_readonly_baseline' and 'not semantic' in item['limitation']
    assert material['sources']['workspace.json']['text'] == (project.root/'workspace.json').read_text()
    assert material['sources']['parameters/input.json']['text'] == (project.root/'parameters/input.json').read_text()
    assert '# candidate change still shown in full' in material['sources']['orders/rules.py']['text']
    assert '# candidate change still shown in full' in material['differences'][0]['diff']
    assert material['original_goal'] == 'Review an owned binary input fixture'
    assert material['review_rules'] == list(claims())
    assert 'fixture' in material['verification']['commands'][0]['log_tail']
    assert 'standard' in material['applicable_sources']
    deployment, session = setup_review(api, monkeypatch, tmp_path)
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['projection']['gates']['review'] == 'passed', result
    assert api.evaluate_acceptance('session')['development_state'] == 'accepted'
    target = next(name for name in opaque if name.startswith('data/source.lance/'))
    with (project.root/target).open('ab') as stream: stream.write(b'changed')
    stale = api.status('session')
    assert not stale['ready'] and stale['development_state'] != 'accepted'
    assert stale['gates']['review'] == 'stale'


def test_readonly_binary_mutation_during_review_collection_fails_closed(readonly_data, monkeypatch):
    from demiflow.forge.development import managed as implementation
    api, project = readonly_data
    current = ready(api)
    original = implementation._readonly_review_artifact
    changed = False
    def mutate(*args, **kwargs):
        nonlocal changed
        result = original(*args, **kwargs)
        if not changed:
            changed = True
            with (project.root/args[1]).open('ab') as stream: stream.write(b'changed')
        return result
    monkeypatch.setattr(implementation, '_readonly_review_artifact', mutate)
    with pytest.raises(ForgeError, match='changed'):
        api.review_material('session', current['source_fingerprint'])


def test_unreviewable_writable_binary_candidate_never_becomes_opaque(managed):
    api, project, _ = managed
    (project.root/'orders/new.bin').write_bytes(b'\xff\x00\xfe')
    current = ready(api)
    with pytest.raises(ForgeError, match='UTF-8'):
        api.review_material('session', current['source_fingerprint'])


@pytest.mark.parametrize('suffix', ['.py', '.json', '.txt'])
def test_readonly_code_config_and_norm_extensions_still_require_complete_text(tmp_path, suffix):
    root = tmp_path/'project'; root.mkdir()
    project = fixture(root)
    path = root/('protected'+suffix); path.write_bytes(b'\xff\x00')
    config = json.loads(project.path.read_text()); config['readonly'] = [path.name]
    project.path.write_text(json.dumps(config))
    api = ManagedTasks(tmp_path/'state','fixture',root,review_authority=ReviewAuthority())
    api.prepare_task('session','orders','Keep required text visible',['orders'],['No production'],
        contract=dict(allow_tests=True,max_verification_seconds=5,developer_session_id='developer'))
    current = ready(api)
    with pytest.raises(ForgeError, match='UTF-8'):
        api.review_material('session', current['source_fingerprint'])


@pytest.mark.parametrize('mode', ['reasoning', 'reasoning_truncated'])
def test_real_native_reasoning_sequence_requires_a_complete_text_verdict(managed, monkeypatch, tmp_path, mode):
    api, _, _ = managed
    current = ready(api)
    deployment, session = setup_review(api, monkeypatch, tmp_path, mode=mode)
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['dispatch'] == 'acknowledged' and result['retryable'] is False, result
    record = json.loads((api.store.directory('session')/'task.json').read_text())
    if mode == 'reasoning':
        assert result['projection']['gates']['review'] == 'passed'
        assert 'receipt' in record['review']
    else:
        assert result['projection']['gates']['review'] == 'unavailable'
        assert 'token budget exhausted' in record['review']['reason']
        assert 'receipt' not in record['review']


def test_independent_review_effort_does_not_modify_developer_grant(managed, monkeypatch, tmp_path):
    api, _, _ = managed
    current = ready(api)
    deployment, session = setup_review(api, monkeypatch, tmp_path)
    original = dict(deployment.providers['deepseek'])
    assert original['effort'] == 'high'
    result = review.review_candidate(deployment,None,{},session,{},api,current,time.time()+10)
    assert result['projection']['gates']['review'] == 'passed', result
    # The launch fixture checks low and the native stream checks low/16384;
    # subsequent coding segments retain the exact original provider mapping.
    assert deployment.providers['deepseek'] == original
