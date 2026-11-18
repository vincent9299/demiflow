import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from demiflow.workbench.conversations.codex import Codex
from demiflow.workbench.conversations.deepseek import DeepSeek
from demiflow.workbench.conversations.transport import Transport
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


def test_acp_capabilities_explicit_intervals_and_permission_context():
    wire=Wire();provider=DeepSeek(wire,{'model':'fixture'},{'root':'/tmp/project'})
    provider.open('Required source text')
    provider.opened({'sessionId':'session','configOptions':[{'id':'model','currentValue':'["deepseek-official","fixture"]'}]})
    key=provider.start('Human task','submission')
    assert provider.turn=='acp-interval:1'
    assert 'Required source text' in wire.messages[-1]['params']['prompt'][0]['text']
    assert provider.event({'method':'session/update','params':{'sessionId':'session','update':{'sessionUpdate':'agent_thought_chunk','content':{'type':'text','text':'private'}}}})==[]
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


def test_acp_mcp_bridge_uses_public_registered_cli_and_delivery_gate(tmp_path):
    from pathlib import Path
    from test_forge_development import DevelopmentTests
    from test_forge_development_chat import configuration
    from demiflow.workbench.conversations.service import Service
    from demiflow.workbench.conversations.context import assemble, ProtocolBridge, BridgeServer
    from demiflow.workbench.conversations.common import root_identity
    from demiflow.workbench.conversations.common import wire
    from demiflow.workbench.conversations import mcp_bridge
    fixture = DevelopmentTests(); fixture.setUp()
    _, config_path, config = configuration(tmp_path)
    project = dict(root=str(fixture.root), kind='demiforge_registered', phase='implementation',
                   writes=['orders'], watch=['orders'], sources=[], task_id='test', pipeline='orders',
                   non_goals=['production'], authorization='Registered fixture only')
    config['projects'] = {'wregistered': project}
    config_path.write_text(json.dumps(config))
    service = Service(config_path)
    child = bridge = None
    try:
        sid = service.create('wregistered', fixture.root, 'mcp', 'codex')['id']
        turn, _ = service.store.submit('wregistered', sid, 'bridge', 0, 'Read and check the existing fixture',
                                       root_identity(fixture.root), service.policy_hash)
        scratch = service.root / sid / turn['id']; scratch.mkdir(parents=True)
        manifest, _ = assemble(project, turn['text'], scratch, service.deployment.limits)
        service.store.update(turn['id'], context=wire(manifest).decode(), delivery='acknowledged')
        bridge = BridgeServer(ProtocolBridge(service.store, turn['id'], project, scratch),
                              epoch=turn['epoch'], deadline=time.time()+35)
        child = subprocess.Popen([sys.executable, '-I', '-B', mcp_bridge.__file__, '--connect',
                                  str(bridge.path), bridge.token, turn['id'], turn['epoch']],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        serial = 0
        def rpc(method, params):
            nonlocal serial
            serial += 1
            child.stdin.write(wire(dict(jsonrpc='2.0', id=serial, method=method, params=params)) + b'\n'); child.stdin.flush()
            # Enforce a wall-clock deadline as well as the MCP frame limit.
            import select
            assert select.select([child.stdout], [], [], 35)[0], 'MCP response timeout'
            raw = child.stdout.readline(65537)
            assert raw.endswith(b'\n') and len(raw) <= 65536
            return json.loads(raw)
        def operation(name, arguments):
            return rpc('tools/call', dict(name='development_protocol', arguments=dict(operation=name, arguments=arguments)))
        assert rpc('initialize', dict(protocolVersion='2024-11-05'))['result']['serverInfo']['name'] == 'demiflow-development'
        checked = operation('check', {})
        assert json.loads(checked['result']['content'][0]['text'])['passed']
        assert 'error' in operation('handoff', {'accept': True})
        operation('refresh', {'reason': 'Fixture delivery renewal'})
        assert service.store.turn(turn['id'])['delivery'] == 'stale'
        assert 'error' in operation('check', {})
        listing = json.loads(operation('context', {})['result']['content'][0]['text'])
        for name, source in listing['sources'].items():
            for number in range(1, source['pages'] + 1):
                page = json.loads(operation('context_read', {'source': name, 'page': number})['result']['content'][0]['text'])
                assert 'error' in operation('ack_context', {'digest': 'unknown'})
                assert 'result' in operation('ack_context', {'digest': page['delivery_digest']})
        assert service.store.turn(turn['id'])['delivery'] == 'acknowledged'
        assert json.loads(operation('check', {})['result']['content'][0]['text'])['passed']
        service.store.update(turn['id'], state='completed', cleanup='complete')
    finally:
        if child is not None:
            child.terminate(); child.wait(timeout=5)
            child.stdin.close(); child.stdout.close(); child.stderr.close()
        if bridge is not None: bridge.close()
        service.close(); fixture.doCleanups()
