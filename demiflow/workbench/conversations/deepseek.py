"""DeepSeek Harness 0.2.1-rc.2 ACP adapter, with explicit capability differences.

No elicitation or provider transcript replay. ACP prompt acknowledgement is the
first correlated activity or its final response, not session/new or a local write.
"""
from __future__ import annotations

import json
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
        if sum(bool(config.get(k)) for k in ('_compaction', '_review', '_model_broker')) > 1:
            reject('A provider process cannot compact and review at once')
        if boundary is not None and (type(boundary) is not dict or set(boundary) != {'token'} or
                type(boundary['token']) is not str or len(boundary['token']) != 64):
            reject('Invalid fixed model channel')
        profile_name='devchat-model-broker' if config.get('_model_broker') else 'devchat-review' if config.get('_review') else 'devchat-compact' if config.get('_compaction') else 'devchat'
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
        if config.get('_review'):
            # A trusted one-shot LLM service request: no agent/session or tool
            # execution loop. The plugin location is fixed by this package.
            timeout = config.get('_review_timeout_ms', 180000)
            if type(timeout) is not int or not 1 <= timeout <= 180000:
                reject('Invalid independent review deadline')
            overlay = [row for row in overlay if row['id'] != 'acp']
            overlay += [dict(id='acp',disabled=True),dict(id='llm-retry',disabled=True),dict(insert=[dict(id='demiflow-review',
                name=str(Path(__file__).with_name('deepseek_review.cjs')),
                config=dict(provider=route,model=config['model'],reasoningEffort=config.get('effort','high'),
                            timeoutMs=timeout,maxOutputBytes=65536))])]
        if config.get('_model_broker'):
            overlay = [row for row in overlay if row['id'] != 'acp']
            overlay += [dict(id='acp',disabled=True),dict(id='llm-retry',disabled=True),dict(insert=[dict(id='demiflow-model-broker',
                name=str(Path(__file__).with_name('deepseek_model_broker.cjs')),
                config=dict(provider=route,model=config['model'],reasoningEffort=config.get('effort','high')))])]
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
        self.prompt='';self.prompt_key=None;self.items={};self.acknowledged=False
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
        self.prompt=prompt
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
        # Context and requirement share one immutable prompt frame. A lost send
        # cannot be retried automatically; no separate model title request exists.
        self.prompt_key=self.transport.request('session/prompt',dict(sessionId=self.thread,prompt=[dict(type='text',text=self.prompt+'\n\n当前请求：\n'+message)]))
        self.turn='acp-interval:'+self.prompt_key
        return self.prompt_key

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
        if method!='session/update':return []
        update=params.get('update',{});kind=update.get('sessionUpdate')
        if kind=='agent_thought_chunk':return []
        if kind=='agent_message_chunk':
            content=update.get('content',{})
            if content.get('type')!='text':reject('Only text development messages are supported')
            return [('activity',{}),('message.delta',dict(item_id=self.turn+':answer',text=content['text']))]
        if kind in {'tool_call','tool_call_update'}:
            key=update['toolCallId']
            if len(self.items)>=128 and key not in self.items:reject('ACP tool count exceeded budget')
            item={**self.items.get(key,{}),**update}
            # Retain only approval context, never all prior tool output frames.
            remembered={k:v for k,v in item.items() if k in {'toolCallId','title','rawInput','kind','locations'}}
            wire(remembered, 16384)
            self.items[key]=remembered
            return [('activity',{}),('item.updated',dict(id=key,type='tool',**{k:v for k,v in item.items() if k in {'title','status','rawInput','locations','content'}}))]
        if kind=='usage_update':return [('usage.observed',{k:v for k,v in update.items() if k!='sessionUpdate'})]
        return []

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
