"""Codex app-server adapter pinned to qualified versions; Codex owns its model/tool loop."""
from __future__ import annotations

import json
import os
from pathlib import Path

from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import text
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.policy import proxy_environment
from demiflow.workbench.conversations.versions import matches_user_agent


class Codex:
    name = 'codex'

    @staticmethod
    def launch(config, home, root):
        # Project configuration can add merged integrations. A separately reviewed
        # task must remove/relocate it; an empty override is not a clearing proof.
        for parent in (Path(root), *Path(root).parents):
            if (parent / '.codex/config.toml').exists():
                reject('Project Codex configuration requires a reviewed isolated profile')
        boundary = config.get('_boundary')
        if boundary is not None and (type(boundary) is not dict or set(boundary) != {'token'} or
                type(boundary['token']) is not str or len(boundary['token']) != 64 or
                any(config.get(k) for k in ('base_url', 'credential_env', 'native_credentials', 'inherit_proxy'))):
            reject('Restricted Codex requires a fixed task model channel without upstream credentials')
        home = Path(home)
        codex_home = home / 'codex'; codex_home.mkdir(mode=0o700, exist_ok=True)
        env = dict(PATH=str(Path(config['executable']).parent) + ':/usr/local/bin:/usr/bin:/bin',
                   HOME=str(home), CODEX_HOME=str(codex_home), TMPDIR=str(home / 'tmp'), LANG='C.UTF-8',
                   TOKIO_WORKER_THREADS='2', RAYON_NUM_THREADS='2', OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='2')
        (home / 'tmp').mkdir(mode=0o700, exist_ok=True)
        env.update(proxy_environment(config))
        if config.get('node'):
            env['PATH'] = str(Path(config['node']).parent) + ':' + env['PATH']
        values = {
            'model': config['model'], 'model_reasoning_effort': config.get('effort', 'xhigh'),
            'approval_policy': 'on-request', 'approvals_reviewer': 'user',
            'sandbox_mode': 'danger-full-access', 'web_search': 'disabled',
            'shell_environment_policy.inherit': 'none',
            'shell_environment_policy.set': {'PATH': '/usr/bin:/bin', 'HOME': str(home / 'tool-home'), 'TMPDIR': str(home / 'tmp')},
            'features.apps': False, 'features.hooks': False, 'features.multi_agent': False,
            'features.multi_agent_v2': False, 'features.remote_control': False,
            'features.memories': False, 'features.js_repl': False,
            'features.remote_plugin': False, 'features.tool_suggest': False,
            'features.auth_elicitation': False, 'check_for_update_on_startup': False,
            'mcp_servers': {}, 'plugins': {},
        }
        if config.get('_project_python'):
            py_path = Path(config['_project_python'])
            shell = values['shell_environment_policy.set']
            shell.update(PATH=str(py_path.parent) + ':' + shell['PATH'],
                         DEMIFLOW_PROJECT_PYTHON=str(py_path))
        if config.get('_cell_bin'):
            shell = values['shell_environment_policy.set']
            shell.update(PATH=config['_cell_bin'] + ':' + shell['PATH'],
                         DEMIFLOW_CONTROL_ENDPOINT='__DEMIFLOW_CELL_ENDPOINT__')
        (home / 'tool-home').mkdir(mode=0o700, exist_ok=True)
        if config.get('base_url') or boundary:
            values['model_provider'] = 'devchat'
            values.update({'model_providers.devchat.name': 'Configured development provider',
                'model_providers.devchat.base_url': 'http://__DEMIFLOW_MODEL_ENDPOINT__/v1' if boundary else config['base_url'],
                'model_providers.devchat.wire_api': 'responses',
                'model_providers.devchat.requires_openai_auth': False})
            if boundary:
                env['DEMIFLOW_MODEL_TOKEN'] = boundary['token']
                values['model_providers.devchat.env_key'] = 'DEMIFLOW_MODEL_TOKEN'
                values['model_providers.devchat.supports_websockets'] = False
            elif config.get('credential_env'):
                name = config['credential_env']; env[name] = os.environ[name]
                values['model_providers.devchat.env_key'] = name
        elif config.get('credential_env'):
            env['OPENAI_API_KEY'] = os.environ[config['credential_env']]
        def toml(value):
            if isinstance(value, dict):
                return '{' + ', '.join(json.dumps(k) + '=' + toml(v) for k, v in value.items()) + '}'
            return json.dumps(value, ensure_ascii=False)
        command = [config['executable'], 'app-server', '--listen', 'stdio://']
        for key, value in values.items(): command += ['-c', key + '=' + toml(value)]
        return command, env

    def __init__(self, transport, config, project, saved_session=None):
        self.transport, self.config, self.project = transport, config, project
        self.thread, self.turn = saved_session, None
        self.items = {}

    def initialize(self):
        return self.transport.request('initialize', {'clientInfo': {'name': 'demiflow-development', 'version': '1'},
            'capabilities': {'experimentalApi': True}})

    def initialized(self, result):
        if not matches_user_agent('codex', result.get('userAgent', ''),
                                  expected_version=self.config.get('version')):
            reject('Codex runtime version differs from the qualified version')
        self.transport.send(dict(method='initialized', params={}))
        return self.transport.request('config/read', dict(cwd=self.project['root'], includeLayers=True))

    def verify_config(self, result):
        config = result.get('config', {})
        for name in ('mcp_servers', 'plugins'):
            if config.get(name): reject('Unexpected merged provider integrations: ' + name)
        features = config.get('features') or {}
        for key in ('apps', 'hooks', 'multi_agent', 'multi_agent_v2'):
            if features.get(key) is not False: reject('Provider capability was not explicitly disabled: ' + key)
        if config.get('shell_environment_policy', {}).get('inherit') != 'none':
            reject('Provider shell environment is not isolated from credential environment')
        if any(config.get(k) for k in ('notify', 'experimental_thread_store_endpoint', 'model_instructions_file')):
            reject('Unexpected external provider configuration')
        if config.get('web_search') != 'disabled': reject('Native web search is not disabled')
        return dict(integrations='disabled', shell_environment='allowlist', filesystem='host_permissions')

    def open(self, prompt, *, tools=()):
        params = dict(cwd=self.project['root'], model=self.config['model'],
            approvalPolicy='on-request', approvalsReviewer='user', sandbox='danger-full-access', developerInstructions=prompt)
        if self.config.get('base_url') or self.config.get('_boundary'): params['modelProvider'] = 'devchat'
        if self.thread:
            params.update(threadId=self.thread, excludeTurns=True)
            return self.transport.request('thread/resume', params)
        params.update(ephemeral=False, experimentalRawEvents=False, allowProviderModelFallback=False,
                      dynamicTools=[dict(type='function', **tool) for tool in tools])
        return self.transport.request('thread/start', params)

    def opened(self, result):
        if result.get('cwd') != self.project['root'] or result.get('model') != self.config['model']:
            reject('Provider effective cwd/model differs from the authorized request')
        if result.get('approvalPolicy') != 'on-request' or result.get('sandbox', {}).get('type') != 'dangerFullAccess':
            reject('Provider effective permission policy differs from explicit trusted_local mode')
        self.thread = result['thread']['id']
        return self.thread

    def start(self, message, submission):
        return self.transport.request('turn/start', dict(threadId=self.thread,
            input=[dict(type='text', text=message)], clientUserMessageId=submission,
            effort=self.config.get('effort', 'xhigh'), model=self.config['model']))

    def steer(self, message, submission):
        return self.transport.request('turn/steer', dict(threadId=self.thread,expectedTurnId=self.turn,
            input=[dict(type='text',text=message)],clientUserMessageId=submission))

    def started(self, response):
        self.turn = response['turn']['id']
        return self.turn

    def interrupt(self):
        if self.thread and self.turn:
            return self.transport.request('turn/interrupt', dict(threadId=self.thread, turnId=self.turn))

    def inspect(self, cursor=None):
        return self.transport.request('thread/turns/list', dict(threadId=self.thread, limit=10,
            itemsView='summary', **({'cursor': cursor} if cursor else {})))

    def event(self, event):
        method, params = event.get('method'), event.get('params', {})
        if type(method) is not str or type(params) is not dict: reject('Invalid provider event')
        if params.get('threadId') not in {None, self.thread}: reject('Provider thread identity mismatch')
        if self.turn and params.get('turnId') not in {None, self.turn}: reject('Provider turn identity mismatch')
        if 'id' in event:
            if params.get('threadId') != self.thread or not params.get('turnId'):
                reject('Server request lacks current thread/turn identity')
            if method == 'item/tool/call':
                return [('bridge', dict(rpc_id=event['id'], tool=params.get('tool'), arguments=params['arguments']))]
            accepted = {'item/tool/requestUserInput', 'item/commandExecution/requestApproval',
                        'item/fileChange/requestApproval', 'item/permissions/requestApproval'}
            if method not in accepted:
                self.transport.send(dict(id=event['id'], error=dict(code=-32601, message='Unsupported development request')))
                reject('Unsupported provider permission/input request: ' + method)
            allowed = self.approvable(method, params)
            return [('request', dict(rpc_id=event['id'], method=method, params=params, allow_once=allowed))]
        if method == 'turn/started':
            actual = params['turn']['id']
            if self.turn and self.turn != actual: reject('Unexpected overlapping provider turn')
            self.turn = actual
            return []
        if method == 'turn/completed':
            actual = params['turn']['id']
            if self.turn != actual: reject('Unexpected completed turn identity')
            status = params['turn']['status']
            return [('done', dict(status={'completed': 'completed', 'failed': 'failed', 'interrupted': 'interrupted'}.get(status, 'failed'), error=params['turn'].get('error')))]
        if method == 'item/agentMessage/delta':
            return [('message.delta', dict(item_id=params['itemId'], text=params['delta']))]
        if method == 'item/reasoning/summaryTextDelta':
            # Only the public summary channel. Raw reasoning/encrypted content
            # remains outside the transcript and is never reconstructed.
            index=params.get('summaryIndex', 0)
            if type(index) is not int or not 0 <= index < 128:
                reject('Reasoning summary index exceeds budget', 'resource_limit')
            if type(params.get('delta')) is not str: reject('Invalid reasoning summary text')
            return [('message.delta', dict(item_id=params['itemId']+':summary:'+str(index),
                                          text=params['delta'], channel='summary'))]
        if method == 'item/commandExecution/outputDelta':
            return [('output.delta', dict(item_id=params['itemId'], text=params['delta']))]
        if method in {'item/started', 'item/completed'}:
            item = params['item']; kind = item.get('type')
            if kind == 'reasoning':
                if method != 'item/completed': return []
                summary=item.get('summary', [])
                if type(summary) is not list or len(summary)>128:
                    reject('Reasoning summary exceeds budget', 'resource_limit')
                result=[]
                for index, part in enumerate(summary):
                    if type(part) is not str: reject('Invalid public reasoning summary')
                    result.append(('message.completed', dict(item_id=item['id']+':summary:'+str(index),
                                                            text=part, channel='summary')))
                return result
            if kind == 'userMessage': return []
            if len(self.items) >= 128 and item['id'] not in self.items:
                reject('Provider item count exceeded budget', 'resource_limit')
            safe = {k: v for k, v in item.items() if k in {'id', 'type', 'text', 'status', 'command', 'cwd', 'exitCode', 'durationMs', 'changes', 'name'}}
            # File approvals only need names. Completed command output/text is
            # already in the bounded event store and must not accumulate here.
            remembered = dict(id=item['id'], type=kind)
            if kind == 'fileChange':
                remembered['changes'] = [dict(path=c.get('path', '')) for c in item.get('changes', [])]
            wire(remembered, 16384)
            self.items[item['id']] = remembered
            if kind == 'agentMessage': return [('message.completed', dict(item_id=item['id'], text=item.get('text', '')))] if method.endswith('completed') else []
            return [(method.replace('/', '.'), safe)]
        if method == 'thread/tokenUsage/updated': return [('usage.observed', params.get('tokenUsage', {}))]
        if method == 'error': return [('provider.error', {k: params[k] for k in ('error', 'willRetry') if k in params})]
        if method.startswith(('account/', 'mcp', 'permissions/')):
            reject('Unexpected provider identity/integration event: ' + method)
        # Private reasoning and legacy raw-event streams are never persisted.
        return []

    def approvable(self, method, params):
        if method == 'item/tool/requestUserInput': return True
        if method == 'item/commandExecution/requestApproval':
            return bool(params.get('command') and params.get('cwd') == self.project['root'] and
                not any(params.get(k) for k in ('additionalPermissions', 'networkApprovalContext', 'proposedNetworkPolicyAmendments', 'environmentId')))
        if method == 'item/fileChange/requestApproval':
            item = self.items.get(params['itemId'], {})
            paths = [c.get('path', '') for c in item.get('changes', [])]
            return bool(paths) and not params.get('grantRoot') and all(self.writable(p) for p in paths)
        return False

    def writable(self, path):
        p = Path(path)
        if p.is_absolute():
            try: path = str(p.relative_to(self.project['root']))
            except ValueError: return False
        try: safe_path(self.project['root'], path, missing=True)
        except (ValueError, OSError): return False
        return any(path == w or path.startswith(w + '/') for w in self.project['writes'])

    @staticmethod
    def answer(request, answer):
        method, params = request['method'], request['params']
        if method == 'item/tool/requestUserInput':
            questions = params['questions']
            if type(answer) is not dict or set(answer) != {q['id'] for q in questions}:
                reject('Every current input question needs an explicit answer', 'invalid_request')
            return dict(answers={key: dict(answers=[text(value, 8192)]) for key, value in answer.items()})
        if answer not in ('allow_once', 'deny'):
            reject('Choose allow_once or deny', 'invalid_request')
        if answer == 'allow_once' and not request['allow_once']:
            reject('This request exceeds the deployment grant')
        if method == 'item/permissions/requestApproval': return dict(permissions={}, scope='turn')
        return dict(decision='accept' if answer == 'allow_once' else 'decline')


class CodexCompact(Codex):
    """Native maintenance turn; never a slash command sent as a user message."""
    def open(self, prompt, *, tools=()):
        if not self.thread: reject('Compaction requires an existing provider thread')
        return self.transport.request('thread/resume', dict(threadId=self.thread,
            cwd=self.project['root'], model=self.config['model'], excludeTurns=True,
            approvalPolicy='on-request', approvalsReviewer='user', sandbox='danger-full-access',
            **({'modelProvider':'devchat'} if self.config.get('base_url') else {})))

    def start(self, message, submission):
        return self.transport.request('thread/compact/start', dict(threadId=self.thread))

    def started(self, response):
        # The RPC accepts maintenance with {}, turn/started supplies its identity.
        if response != {}: reject('Unexpected compaction acknowledgement')
        return None

    def event(self, event):
        result = super().event(event)
        if event.get('method') == 'item/completed' and event.get('params',{}).get('item',{}).get('type') == 'contextCompaction':
            self.compaction_result = dict(status='compacted', provider='codex')
        return result
