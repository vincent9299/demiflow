"""DeepSeek Harness 0.2.1-rc.2 ACP adapter, with explicit capability differences.

No elicitation or provider transcript replay. ACP prompt acknowledgement is the
first correlated activity or its final response, not session/new or a local write.
"""
from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path

from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import text
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.policy import proxy_environment

DISABLED = (
    'plugin-manager', 'tool-plugin-manager', 'config-editor', 'settings', 'hmr',
    'session-title-llm', 'goal', 'goal-round-driver', 'command-goal', 'tool-goal',
    'subagent', 'subagent-spawn-in-process', 'subagent-fork-in-process',
    'tool-subagent-control', 'tool-subagent-list-agents', 'tool-subagent', 'tool-subagent-fork',
    'workflow-ptc', 'tool-workflow', 'tool-jobs', 'tool-web', 'web-search-deepseek',
    'web-deepseek-api', 'web-fetch-http', 'deepseek-account', 'llm-deepseek-account',
    'credentials', 'authorization', 'tool-working-directory', 'tool-skill', 'skill-filesystem',
    'session-telemetry-otel',
)


class DeepSeek:
    name = 'deepseek'
    acp = True

    @staticmethod
    def launch(config, home, root):
        home=Path(home);dsh_home=home/'dsh'
        boundary = config.get('_boundary')
        if config.get('_compaction') and config.get('_model_broker'):
            reject('A provider process cannot combine compaction and model brokerage')
        if boundary is not None and (type(boundary) is not dict or set(boundary) != {'token'} or
                type(boundary['token']) is not str or len(boundary['token']) != 64):
            reject('Invalid fixed model channel')
        profile_name='devchat-model-broker' if config.get('_model_broker') else 'devchat-compact' if config.get('_compaction') else 'devchat'
        profile=dsh_home/'profiles'/profile_name
        profile.mkdir(parents=True,mode=0o700,exist_ok=True)
        (home/'tmp').mkdir(mode=0o700,exist_ok=True)
        executable=Path(config['executable']).resolve()
        package=executable.parent.parent if executable.name=='bin.js' else None
        if package is not None:
            version=json.loads(read_file(package/'package.json',65536))['version']
            if version!=config['version']:reject('Harness package version differs from qualified version')
        manifest={'name':'demiflow-development-acp','private':True,'dsh':{'profile':{'bundles':['@deepseek-ai/dsh-base','@deepseek-ai/dsh-acp-app']}}}
        (profile/'package.json').write_bytes(wire(manifest))
        if boundary and (config.get('native_credentials') or config.get('credential_env') or config.get('base_url') or config.get('inherit_proxy')):
            reject('Restricted CLI configuration must not carry upstream authentication or endpoints')
        enabled = {'credentials'} if config.get('native_credentials') else set()
        route = config.get('model_provider', 'deepseek-official')
        if route == 'deepseek-account':
            if not config.get('native_credentials') and not boundary:
                reject('DeepSeek account route requires an explicit native credential reference')
            if not boundary: enabled.update({'deepseek-account', 'llm-deepseek-account', 'authorization'})
        overlay=[{'id':name,'disabled':True} for name in DISABLED if name not in enabled]
        if config.get('native_credentials'):
            overlay.append(dict(id='credentials', config=dict(path=config['native_credentials'], watch=False)))
        overlay += [dict(id='acp',config=dict(provider=route,model=config['model'],sessionListPageSize=20)),
            dict(id='sandbox-policy',config=dict(mode='danger-full-access',workspaceRoot=root)),
            dict(id='approval',config=dict(policy='ask')),
            dict(id='permission',config=dict(defaultPreset='trusted-local',presets={'trusted-local':dict(sandbox='danger-full-access',approval='ask')})),
            dict(id='llm-deepseek',config=dict(reasoningEffort=config.get('effort','high'),apiKeyEnv='DEEPSEEK_API_KEY',
                **({'baseURL':config['base_url']} if config.get('base_url') else {})))]
        if config.get('_compaction'):
            overlay = [row for row in overlay if row['id'] != 'acp']
            overlay += [dict(id='acp',disabled=True),dict(insert=[dict(id='demiflow-compaction',
                name=str(Path(__file__).with_name('deepseek_compact.cjs')),
                config=dict(cwd=root,provider=route,model=config['model']))])]
        if config.get('_model_broker'):
            overlay = [row for row in overlay if row['id'] != 'acp']
            overlay += [dict(id='acp',disabled=True),dict(id='llm-retry',disabled=True),dict(insert=[dict(id='demiflow-model-broker',
                name=str(Path(__file__).with_name('deepseek_model_broker.cjs')),
                config=dict(provider=route,model=config['model'],reasoningEffort=config.get('effort','high')))])]
        if not any(config.get(key) for key in ('_compaction', '_review', '_model_broker')):
            next(row for row in overlay if row.get('id')=='acp')['inject']=[
                'acpAppStartup','demiflowAssistantStreamReady']
            overlay.append(dict(insert=[dict(id='demiflow-assistant-stream',
                name=str(Path(__file__).with_name('deepseek_stream.cjs')), config=dict(cwd=root,
                    llmModule=str(package/'node_modules/@deepseek-ai/dsh-llm/lib/index.js') if package else None))]))
        if boundary:
            if package is None: reject('Restricted model transport requires the pinned installed Harness')
            overlay += [dict(id='llm-deepseek',disabled=True),dict(insert=[dict(id='demiflow-model-proxy',
                name=str(Path(__file__).with_name('deepseek_model_proxy.cjs')),
                config=dict(provider=route,model=config['model'],token=boundary['token'],
                    llmModule=str(package/'node_modules/@deepseek-ai/dsh-llm/lib/index.js')))])]
        (profile/'cordis.patch.yml').write_bytes(wire(overlay,65536))
        env=dict(PATH=str(Path(config.get('node',config['executable'])).parent)+':/usr/bin:/bin',HOME=str(home),DSH_HOME=str(dsh_home),
                 TMPDIR=str(home/'tmp'),LANG='C.UTF-8',DSH_PERMISSION_MODE='danger-full-access',OMP_NUM_THREADS='2',OPENBLAS_NUM_THREADS='2')
        if config.get('credential_env'):env['DEEPSEEK_API_KEY']=os.environ[config['credential_env']]
        if config.get('_project_python'):
            py_path = Path(config['_project_python'])
            env['PATH'] = str(py_path.parent) + ':' + env['PATH']
            env['DEMIFLOW_PROJECT_PYTHON'] = str(py_path)
        if config.get('_shim_bin'):
            env['PATH'] = str(config['_shim_bin']) + ':' + env['PATH']
        if config.get('_cell_bin'):
            env['PATH'] = config['_cell_bin'] + ':' + env['PATH']
        env.update(proxy_environment(config))
        if config.get('inherit_proxy'):
            env['NODE_USE_ENV_PROXY'] = '1'
        command=[config['executable'],'--profile',profile_name]
        if package is not None:
            node = config.get('node')
            if not node or not Path(node).is_file():
                reject('Pinned Harness requires its installed Node executable')
            command = [node, '--require', str(Path(__file__).with_name('deepseek_stdio.cjs')),
                       str(executable), '--profile', profile_name]
        return command,env

    def __init__(self,transport,config,project,saved_session=None):
        self.transport,self.config,self.project=transport,config,project
        self.thread,self.turn=saved_session,None
        self.prompt='';self.prompt_key=None;self.prompt_sent=False;self.items={};self.acknowledged=False
        self.message_scope=None;self.message_block=0;self.message_kind=None
        self.stream_attempt=None;self.stream_blocks={};self.stream_count=0;self.stream_messages=set()
        self.metrics_sequence=-1
        self.mcp_servers=[]

    def initialize(self):
        return self.transport.request('initialize',dict(protocolVersion=1,clientCapabilities={},clientInfo=dict(name='demiflow-development',version='1')))

    def initialized(self,result):
        if result.get('protocolVersion')!=1 or result.get('agentInfo',{}).get('name')!='deepseek-harness-acp':
            reject('Unsupported Harness ACP handshake')
        capabilities=result.get('agentCapabilities',{}).get('sessionCapabilities',{})
        if any(k not in capabilities for k in ('resume','close')):reject('Harness lacks required lifecycle capability')
        return None

    def verify_config(self,result):
        return dict(profile='private_fixed_acp',filesystem='host_permissions',input_requests='unsupported',history_replay='unsupported')

    def open(self,prompt):
        # Inject Skill only on a fresh session; resumed sessions already carry it.
        self.prompt = '' if self.thread else prompt
        self.prompt_sent = bool(self.thread)
        params=dict(cwd=self.project['root'],mcpServers=self.mcp_servers)
        if self.thread:params['sessionId']=self.thread
        return self.transport.request('session/resume' if self.thread else 'session/new',params)

    def opened(self,result):
        self.thread=result.get('sessionId',self.thread)
        if not self.thread:reject('ACP did not bind a session identity')
        model=next((o.get('currentValue') for o in result.get('configOptions',[]) if o.get('id')=='model'),None)
        if model!=json.dumps([self.config.get('model_provider', 'deepseek-official'),self.config['model']],separators=(',',':')):
            reject('ACP effective model differs from configured model')
        return self.thread

    def start(self,message,submission):
        # Context and requirement share one immutable prompt frame on the first turn;
        # subsequent turns in the same session send only the user's message.
        scope=hashlib.sha256(wire([self.thread, text(submission, 512)])).hexdigest()[:32]
        if self.prompt and not self.prompt_sent:
            body = self.prompt + '\n\n' + message
            self.prompt_sent = True
        else:
            body = message
        self.prompt_key=self.transport.request('session/prompt',dict(sessionId=self.thread,prompt=[dict(type='text',text=body)]))
        self.turn='acp-interval:'+self.prompt_key
        # ACP request IDs restart in each process. They are not persisted message
        # identities: one conversation can contain many "client-3" intervals.
        self.message_scope=scope
        self.message_block=0;self.message_kind=None
        self.stream_attempt=None;self.stream_blocks={};self.stream_count=0;self.stream_messages.clear()
        self.metrics_sequence=-1
        return self.prompt_key

    def steer(self, message, submission):
        self.transport.send(dict(method='demiflow/steer',params=dict(sessionId=self.thread,
            promptId=self.prompt_key,submission=submission,text=message)))

    def interrupt(self):
        self.transport.send(dict(method='session/cancel',params=dict(sessionId=self.thread)))

    def close_request(self):return self.transport.request('session/close',dict(sessionId=self.thread))

    def completion(self,response):
        stop=response.get('stopReason')
        return dict(status='cancelled' if stop=='cancelled' else 'completed' if stop in {'end_turn','stop_sequence'} else 'failed',stop_reason=stop)

    def event(self,event):
        method,params=event.get('method'),event.get('params',{})
        if params.get('sessionId')!=self.thread:reject('ACP session identity mismatch')
        if method=='session/request_permission':
            call=params.get('toolCall',{});item=self.items.get(call.get('toolCallId'))
            # A bare toolCallId is not meaningful permission context.
            allowed=bool(item and item.get('rawInput'))
            return [('request',dict(rpc_id=event['id'],method=method,params=dict(params,known_tool=item),allow_once=allowed))]
        if 'id' in event:reject('Unsupported ACP server request')
        if method=='demiflow/steer-result':
            if params.get('promptId')!=self.prompt_key or params.get('status') not in ('accepted','missed','unknown'):
                reject('Invalid steering acknowledgement')
            return [('steer.result',dict(submission=params['submission'],status=params['status']))]
        if method=='demiflow/metrics':
            from .stats import validate_metric
            if not self.message_scope or set(params)!={'sessionId','metrics'}:reject('Invalid native metric scope')
            metric=validate_metric(params['metrics'])
            if metric['event_sequence']<=self.metrics_sequence:return []
            self.metrics_sequence=metric['event_sequence']
            return [('usage.observed',metric)]
        if method=='demiflow/assistant-stream':return self._stream(params)
        if method!='session/update':return []
        update=params.get('update',{});kind=update.get('sessionUpdate')
        if kind in {'agent_message_chunk', 'agent_thought_chunk'}:
            if type(update.get('messageId')) is str and update['messageId'] in self.stream_messages:return []
            content=update.get('content',{})
            if content.get('type')!='text':reject('Only text development messages are supported')
            if type(content.get('text')) is not str:reject('Invalid ACP message text')
            message_id=update.get('messageId')
            if message_id is not None: text(message_id, 512)
            marker=(kind, message_id)
            if marker!=self.message_kind:
                self.message_block+=1;self.message_kind=marker
            # The pinned third-party ACP adapter explicitly publishes thought
            # blocks. Preserve that public channel separately from its answer.
            channel='process' if kind=='agent_thought_chunk' else 'answer'
            return [('activity',{}),('message.delta',dict(
                item_id='acp:'+self.message_scope+':'+str(self.message_block),
                text=content['text'], channel=channel))]
        if kind in {'tool_call','tool_call_update'}:
            self.message_kind=None
            key=update['toolCallId']
            if len(self.items)>=512 and key not in self.items:
                self.items.pop(next(iter(self.items)))
            item={**self.items.get(key,{}),**update}
            # Bound large tool argument/output previews (e.g. writing a 30 KiB file)
            # so event persistence & approval context never blow the per-event byte budget.
            def _clip(value, max_str=2048, depth=0):
                if depth > 6:
                    return '…'
                if isinstance(value, str):
                    if len(value) > max_str:
                        return value[:max_str] + f'… ({len(value)} chars)'
                    return value
                if isinstance(value, list):
                    return [_clip(v, max_str, depth + 1) for v in value[:32]]
                if isinstance(value, dict):
                    return {str(k)[:128]: _clip(v, max_str, depth + 1) for k, v in list(value.items())[:32]}
                return value
            remembered={k:_clip(v, 1024) for k,v in item.items() if k in {'toolCallId','title','rawInput','kind','locations'}}
            try:
                wire(remembered, 16384)
            except Exception:
                remembered={k:_clip(v, 256) for k,v in item.items() if k in {'toolCallId','title','kind'}}
            self.items[key]=remembered
            payload=dict(id=key,type='tool',**{k:_clip(v, 4096) for k,v in item.items() if k in {'title','status','rawInput','locations','content'}})
            try:
                wire(payload, 49152)
            except Exception:
                payload=dict(id=key,type='tool',title=_clip(item.get('title'), 256),status=item.get('status'),
                             rawInput=_clip(item.get('rawInput'), 512))
            return [('activity',{}),('item.updated',payload)]
        if kind=='usage_update':return [('usage.observed',{k:v for k,v in update.items() if k!='sessionUpdate'})]
        return []

    def _stream(self, params):
        """Normalize only the fixed plugin's public text projection, not raw DSH frames."""
        phase=params.get('phase')
        if phase=='error':reject('Native public stream exceeded its protocol or resource boundary', 'resource_limit')
        if not self.message_scope:reject('Native stream arrived outside a submitted prompt')
        attempt=text(params.get('attemptId'),256)
        if phase=='start':
            if set(params)!={'sessionId','phase','attemptId'} or self.stream_attempt is not None:
                reject('Invalid native assistant attempt boundary')
            self.stream_count+=1
            self.stream_attempt=attempt;self.stream_blocks={}
            return [('activity',{})]
        if attempt!=self.stream_attempt:reject('Native assistant attempt identity mismatch')
        if phase=='settled':
            if set(params)-{'sessionId','phase','attemptId','messageId','status'} or params.get('status') not in {'complete','incomplete'}:
                reject('Invalid native assistant settlement')
            if 'messageId' in params:
                self.stream_messages.add(text(params['messageId'],256))
                if len(self.stream_messages)>1024:
                    self.stream_messages.pop()
            self.stream_attempt=None;self.stream_blocks={}
            return []
        expected={'sessionId','phase','attemptId','blockIndex','channel','text'}
        if phase=='completed':expected|={'status','interrupted'}
        if phase not in {'delta','completed'} or set(params)!=expected:
            reject('Unknown native public stream projection')
        index=params['blockIndex'];channel=params['channel'];content=params['text']
        if (type(index) is not int or not 0<=index<65536 or channel not in {'answer','process'} or
                type(content) is not str or len(content.encode())>128*1024):
            reject('Invalid native public text block')
        if index in self.stream_blocks and self.stream_blocks[index]!=channel:
            reject('Native assistant block channel changed')
        self.stream_blocks[index]=channel
        if len(self.stream_blocks)>64:reject('Native assistant block budget exceeded', 'resource_limit')
        data=dict(item_id='dsh:'+self.message_scope+':'+attempt+':'+str(index),text=content,channel=channel)
        if phase=='completed':
            if (params['status'] not in {'streaming','complete','incomplete'} or
                    type(params['interrupted']) is not bool or params['interrupted']!=(params['status']=='incomplete')):
                reject('Invalid native assistant completion state')
            data.update(status=params['status'],interrupted=params['interrupted'])
        return [('activity',{}),('message.delta' if phase=='delta' else 'message.completed',data)]

    @staticmethod
    def answer(request,answer):
        if answer not in ('allow_once','deny'):reject('ACP expects an explicit permission decision')
        if answer=='allow_once' and not request['allow_once']:reject('ACP tool arguments are not known; permission denied')
        options=request['params'].get('options',[])
        kind='allow_once' if answer=='allow_once' else 'reject_once'
        choice=next((o for o in options if o.get('kind')==kind),None)
        if choice is None:return dict(outcome=dict(outcome='cancelled'))
        return dict(outcome=dict(outcome='selected',optionId=choice['optionId']))


class DeepSeekCompact(DeepSeek):
    """The pinned Harness native compaction service, in the same private home."""
    @staticmethod
    def launch(config, home, root):
        return DeepSeek.launch(dict(config, _compaction=True),home,root)

    def initialized(self, result):
        if result.get('protocolVersion')!=1 or result.get('agentInfo',{}).get('name')!='demiflow-deepseek-compaction':
            reject('Unsupported native compaction handshake')
        return None

    def open(self, prompt):
        if not self.thread: reject('Compaction requires an existing provider session')
        return self.transport.request('session/resume',dict(sessionId=self.thread,cwd=self.project['root']))

    def start(self, message, submission):
        self.prompt_key=self.transport.request('session/compact',dict(sessionId=self.thread))
        self.turn='acp-interval:'+self.prompt_key
        return self.prompt_key

    def completion(self, response):
        if response.get('status') not in {'compacted','noop'}: reject('Native compaction was not confirmed')
        wire(response,4096)
        self.compaction_result=response
        return dict(status='completed')
