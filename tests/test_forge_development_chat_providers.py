import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from demiflow.workbench.conversations.codex import Codex
from demiflow.workbench.conversations.deepseek import DeepSeek
from demiflow.forge.bridge.transport import Transport
from demiflow.workbench.conversations.policy import Limits
from demiflow.forge.schema import ForgeError


class Wire:
    def __init__(self):self.messages=[]
    def send(self,v):self.messages.append(v)
    def request(self,m,p):self.messages.append({'method':m,'params':p});return '1'


def test_codex_approvals_no_expansion_and_foreign_events(tmp_path):
    wire=Wire();provider=Codex(wire,{'model':'fixture'},{'root':str(tmp_path),'writes':['code.py']})
    provider.thread='thread';provider.turn='turn'
    params={'threadId':'thread','turnId':'turn','itemId':'cmd','command':'python test.py','cwd':str(tmp_path)}
    event={'method':'item/commandExecution/requestApproval','id':1,'params':params}
    request=provider.event(event)[0][1]
    assert Codex.answer(request,'allow_once')=={'decision':'accept'}
    params['additionalPermissions']={'network':{'enabled':True}}
    request=provider.event(event)[0][1]
    assert not request['allow_once']
    with pytest.raises(ForgeError):Codex.answer(request,'allow_once')
    assert Codex.answer(request,'deny')=={'decision':'decline'}
    params['turnId']='foreign'
    with pytest.raises(ForgeError,match='identity'):provider.event(event)
    assert provider.event({'method':'item/completed','params':{'threadId':'thread','turnId':'turn','item':{'type':'reasoning','id':'private','text':'DO NOT KEEP'}}})==[]


def test_codex_writable_approves_empty_pipeline_project_and_rejects_protected(tmp_path):
    impl = Codex(Wire(), {'model': 'fixture'}, {
        'root': str(tmp_path), 'kind': 'pipeline', 'phase': 'implementation', 'writes': [], 'protected': ['docs/lock.md'],
    })
    assert impl.writable('flows/demo.py')
    assert impl.writable('pyproject.toml')
    assert impl.writable('.env.example')
    assert impl.writable(str(tmp_path / 'operators' / 'ops.py'))
    for bad in ('.env', '.env.local', '.env.prod', '.forge/state.json', 'workspace.json',
                'AGENTS.md', 'PIPELINE_SPEC.md', 'packages/pkg/a.py', 'data_tools/a.py',
                'docs/lock.md', '../escape.py', '/etc/passwd'):
        assert not impl.writable(bad), bad
    ro = Codex(Wire(), {'model': 'fixture'}, {
        'root': str(tmp_path), 'kind': 'pipeline', 'phase': 'implementation', 'writes': [], 'readonly': True,
    })
    assert not ro.writable('flows/demo.py')
    design = Codex(Wire(), {'model': 'fixture'}, {
        'root': str(tmp_path), 'kind': 'pipeline', 'phase': 'design', 'writes': ['README.md'],
    })
    assert design.writable('README.md') and not design.writable('flows/demo.py')


def test_acp_capabilities_explicit_intervals_and_permission_context():
    wire=Wire();provider=DeepSeek(wire,{'model':'fixture'},{'root':'/tmp/project'})
    provider.open('Required source text')
    provider.opened({'sessionId':'session','configOptions':[{'id':'model','currentValue':'["deepseek-official","fixture"]'}]})
    key=provider.start('Human task','submission')
    assert provider.turn=='acp-interval:1'
    assert 'Required source text' in wire.messages[-1]['params']['prompt'][0]['text']
    thought=provider.event({'method':'session/update','params':{'sessionId':'session','update':{'sessionUpdate':'agent_thought_chunk','content':{'type':'text','text':'Public ACP progress'}}}})
    assert thought[1][0]=='message.delta' and thought[1][1]['channel']=='process'
    assert thought[1][1]['text']=='Public ACP progress'
    request={'method':'session/request_permission','id':'p','params':{'sessionId':'session','toolCall':{'toolCallId':'call'},'options':[{'kind':'allow_once','optionId':'allow'},{'kind':'reject_once','optionId':'reject'}]}}
    pending=provider.event(request)[0][1]
    assert not pending['allow_once']
    with pytest.raises(ForgeError):provider.answer(pending,'allow_once')
    assert provider.answer(pending,'deny')['outcome']['optionId']=='reject'
    provider.event({'method':'session/update','params':{'sessionId':'session','update':{'sessionUpdate':'tool_call','toolCallId':'call','rawInput':{'command':'python test.py'}}}})
    assert provider.event(request)[0][1]['allow_once']
    provider.interrupt();assert wire.messages[-1]['method']=='session/cancel'
    provider.close_request();assert wire.messages[-1]['method']=='session/close'
    assert provider.completion({'stopReason':'cancelled'})['status']=='cancelled'
    with pytest.raises(ForgeError):provider.event({'method':'session/update','params':{'sessionId':'other'}})


def test_acp_public_messages_are_separate_across_processes_tools_and_channels():
    def fresh(submission):
        provider=DeepSeek(Wire(),{'model':'fixture'},{'root':'/tmp/project'},saved_session='same-session')
        provider.start('New requirement',submission)
        return provider
    def chunk(provider,kind='agent_message_chunk',message_id=None):
        update=dict(sessionUpdate=kind,content=dict(type='text',text='Visible provider text'))
        if message_id is not None:update['messageId']=message_id
        return provider.event(dict(method='session/update',params=dict(sessionId='same-session',update=update)))[1][1]
    first=fresh('first-submission');second=fresh('second-submission')
    # Native request IDs reset after each worker launch, including after repairs.
    assert first.turn==second.turn=='acp-interval:1'
    initial=chunk(first)
    assert initial['item_id']==chunk(first)['item_id']
    assert initial['item_id']!=chunk(second)['item_id']
    first.event(dict(method='session/update',params=dict(sessionId='same-session',update=dict(
        sessionUpdate='tool_call',toolCallId='tool',title='Inspect data',status='completed'))))
    final=chunk(first)
    assert final['item_id']!=initial['item_id']
    thought=chunk(first,'agent_thought_chunk','provider-message')
    answer=chunk(first,message_id='provider-message')
    assert thought['channel']=='process' and answer['channel']=='answer'
    assert thought['item_id']!=answer['item_id']
    assert chunk(first,message_id='next-message')['item_id']!=answer['item_id']


def test_codex_keeps_public_summary_but_never_private_reasoning(tmp_path):
    provider=Codex(Wire(),{'model':'fixture'},{'root':str(tmp_path),'writes':['code.py']})
    provider.thread='thread';provider.turn='turn'
    def event(method,**values):
        return provider.event(dict(method=method,params=dict(threadId='thread',turnId='turn',**values)))
    delta=event('item/reasoning/summaryTextDelta',itemId='reason',summaryIndex=0,delta='Check the project.')[0]
    assert delta[0]=='message.delta' and delta[1]['channel']=='summary'
    assert event('item/reasoning/textDelta',itemId='reason',contentIndex=0,delta='PRIVATE')==[]
    assert event('item/started',item=dict(type='reasoning',id='reason',summary=[],content=['PRIVATE']))==[]
    completed=event('item/completed',item=dict(type='reasoning',id='reason',summary=['Check the project.'],content=['PRIVATE']))[0]
    assert completed[0]=='message.completed' and completed[1]['item_id']==delta[1]['item_id']
    assert 'PRIVATE' not in json.dumps(completed)
    for index in (-1,128,True):
        with pytest.raises(ForgeError):event('item/reasoning/summaryTextDelta',itemId='reason',summaryIndex=index,delta='Invalid')


def test_transport_predecode_wide_frame_and_aggregate_flood(tmp_path):
    p=subprocess.Popen([sys.executable,'-c','import sys,time;sys.stdout.write("x"*2000);sys.stdout.flush();time.sleep(10)'],
                       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    transport=Transport(p,Limits(frame_bytes=1024))
    try:
        with pytest.raises(ForgeError,match='frame'):
            until=time.monotonic()+2
            while time.monotonic()<until:transport.poll(.1)
    finally:p.kill();p.wait();transport.close()


def test_native_credential_reference_keeps_history_and_secret_out_of_configuration(tmp_path):
    import os
    from demiflow.workbench.conversations.deepseek import DeepSeek
    from demiflow.workbench.conversations.policy import Deployment
    from test_forge_development_chat import configuration
    root, path, value = configuration(tmp_path)
    credential = tmp_path / 'native-auth.yaml'
    secret = 'fixture-secret-not-real'
    credential.write_text('version: 1\nrefs:\n  DEEPSEEK_API_KEY: ' + secret + '\n')
    credential.chmod(0o600)
    config = dict(executable=value['providers']['codex']['executable'], version='0.2.1-rc.2',
                  model='deepseek-v4-flash', native_credentials=str(credential))
    value['providers'] = {'deepseek': config}
    Deployment(value)
    before = credential.stat()
    home = tmp_path / 'private-home'
    command, env = DeepSeek.launch(config, home, str(root))
    overlay = (home / 'dsh/profiles/devchat/cordis.patch.yml').read_text()
    assert secret not in overlay and secret not in json.dumps(env)
    assert str(credential) in overlay
    assert env['DSH_HOME'] == str(home / 'dsh')
    assert not (home / 'dsh/.credentials.yaml').exists()
    assert credential.stat().st_mtime_ns == before.st_mtime_ns
    credential.chmod(0o644)
    with pytest.raises(ForgeError, match='private'): Deployment(value)
    credential.chmod(0o600)
    config['native_credentials'] = str(root / 'AGENTS.md')
    (root / 'AGENTS.md').chmod(0o600)
    with pytest.raises(ForgeError, match='outside development projects'): Deployment(value)


@pytest.mark.parametrize('route', ['deepseek-official', 'deepseek-account'])
def test_model_broker_budget_reaches_native_adapter(tmp_path, route):
    config = dict(executable=sys.executable, version='0.2.1-rc.2', model='fixture',
        model_provider=route, native_credentials=str(tmp_path/'credentials.yaml'), effort='high')
    home = tmp_path/'author'
    DeepSeek.launch(config, home, str(tmp_path))
    author = json.loads((home/'dsh/profiles/devchat/cordis.patch.yml').read_text())
    assert not any('streamIdleTimeoutMs' in row.get('config', {}) for row in author)
    home = tmp_path/'review-broker'
    DeepSeek.launch(dict(config, _model_broker=True, _request_seconds=2700), home, str(tmp_path))
    rows = json.loads((home/'dsh/profiles/devchat-model-broker/cordis.patch.yml').read_text())
    native = next(row['config'] for row in rows if row['id'] == (
        'llm-deepseek-account' if route == 'deepseek-account' else 'llm-deepseek'))
    assert native['streamIdleTimeoutMs'] == 2700000
    assert native['reasoningEffort'] == 'high'


def test_proxy_opt_in_and_console_diagnostics_do_not_pollute_acp(tmp_path, monkeypatch):
    from demiflow.workbench.conversations.policy import proxy_environment
    from demiflow.workbench.conversations.deepseek import DeepSeek
    monkeypatch.setenv('HTTPS_PROXY', 'http://fixture-proxy.invalid:8080')
    monkeypatch.setenv('UNRELATED_SECRET', 'not-passed')
    assert proxy_environment({}) == {}
    selected = proxy_environment({'inherit_proxy': True})
    assert selected['HTTPS_PROXY'] == 'http://fixture-proxy.invalid:8080'
    assert 'UNRELATED_SECRET' not in selected
    import shutil
    node = shutil.which('node') or '/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/node'
    from pathlib import Path
    if not Path(node).is_file(): pytest.skip('Node not installed')
    from demiflow.workbench.conversations import deepseek
    preload = Path(deepseek.__file__).with_name('deepseek_stdio.cjs')
    result = subprocess.run([node, '--require', str(preload), '-e',
        'console.info("native catalog diagnostic");process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:"probe",result:{}})+"\\n")'],
        capture_output=True, timeout=10, check=True)
    assert json.loads(result.stdout)['id'] == 'probe'
    assert result.stderr == b'native catalog diagnostic\n'
