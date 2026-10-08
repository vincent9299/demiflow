"""Codex app-server runtime; one ephemeral session and native callbacks per row.

Codex owns the agent loop. This client implements bounded JSON-RPC transport,
not a second model loop. Only declared Dataset operators are dispatched here.
"""
from __future__ import annotations

import asyncio
from functools import partial
import json
import math
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

from demiflow.collect.documents import canonical
from demiflow.collect.reading import PromptContext
from demiflow.environment import LIMITS, STATE, OperatorCallError, _error
from demiflow.objects import LocalObjectStore
from demiflow.operator_media import OperatorImages
from .client import request_messages, _journal_io
from .codex_files import collect_artifacts, verify_artifacts, file_context, context_message
from .errors import PromptBudgetExceededError, PromptResponseContractError
from .model import OperatorLLMResponse, OperatorLLMRequestUsage
from .parser import resolve_prompt
from .sqlite_journal import SQLitePromptJournal, _metadata
from .tokens import CharacterBudget


DEFAULTS = {
    'web_search': 'disabled',
    'shell_tool': False,
    'image_generation': False,
    'view_image': False,
    'max_input_bytes': 16 * 1024**2,
    'max_message_bytes': 4 * 1024**2,
    'max_output_bytes': 16 * 1024**2,
    'max_events': 4096,
    'max_stderr_bytes': 64 * 1024,
    'max_rss_bytes': 2 * 1024**3,
    'max_scratch_bytes': 64 * 1024**2,
}


def validate_options(options):
    if set(options) - {'codex_agent', 'sqlite_journal', 'timeout_s'}:
        raise ValueError('Codex agent accepts codex_agent, sqlite_journal and timeout_s only')
    settings = options.get('codex_agent', {})
    if not isinstance(settings, dict) or set(settings) - set(DEFAULTS) - {
            'bin', 'reasoning_effort', 'model_revision', 'output_schema',
            'artifact_store', 'max_artifact_files', 'max_artifact_bytes'}:
        raise ValueError('Unknown codex_agent settings')
    settings = {**DEFAULTS, **settings}
    for key in DEFAULTS.keys() - {'web_search', 'shell_tool', 'image_generation', 'view_image'}:
        if key == 'max_message_bytes' and settings[key] is None:
            continue
        if type(settings[key]) is not int or settings[key] < 1:
            raise ValueError(key + ' must be a positive integer')
    if settings['max_message_bytes'] is not None and settings['max_message_bytes'] > settings['max_output_bytes']:
        raise ValueError('max_message_bytes must not exceed max_output_bytes')
    if settings['web_search'] not in ('disabled', 'cached', 'live'):
        raise ValueError('Codex web_search must be disabled, cached or live')
    for key in ('shell_tool', 'image_generation', 'view_image'):
        if type(settings[key]) is not bool:
            raise ValueError('Codex ' + key + ' must be boolean')
    artifacts = settings.get('artifact_store')
    if artifacts is not None:
        if (not isinstance(artifacts, dict) or set(artifacts) != {'directory'}
                or not isinstance(artifacts['directory'], str)
                or not Path(artifacts['directory']).is_absolute()):
            raise ValueError('artifact_store requires an absolute directory for independent objects')
        settings['artifact_store'] = {'directory': str(Path(artifacts['directory']).resolve())}
        for key, default in (('max_artifact_files', 8), ('max_artifact_bytes', 64 * 1024**2)):
            settings.setdefault(key, default)
            if type(settings[key]) is not int or settings[key] < 1:
                raise ValueError(key + ' must be a positive integer')
    elif settings['image_generation']:
        raise ValueError('image_generation requires artifact_store for durable output')
    elif {'max_artifact_files', 'max_artifact_bytes'} & set(settings):
        raise ValueError('Artifact limits require artifact_store')
    for key in ('bin', 'reasoning_effort', 'model_revision'):
        if key == 'reasoning_effort' and settings.get(key) is None:
            continue
        if key in settings and (not isinstance(settings[key], str) or not settings[key].strip()):
            raise ValueError(key + ' must be a nonempty string')
    if 'output_schema' in settings and not isinstance(settings['output_schema'], dict):
        raise ValueError('Codex output_schema must be a JSON Schema object')
    timeout = options.get('timeout_s', 600)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('Codex session timeout_s must be finite and positive')
    return settings


def _encode(value, limit):
    """Bound encoded accumulation, including JSON escaping and image data URLs."""
    chunks, size = [], 0
    for part in json.JSONEncoder(ensure_ascii=False, separators=(',', ':'), allow_nan=False).iterencode(value):
        encoded = part.encode('utf-8')
        size += len(encoded)
        if size > limit:
            raise PromptBudgetExceededError('Codex message exceeds configured byte budget')
        chunks.append(encoded)
    return b''.join(chunks)


def _strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key: ' + key)
            result[key] = value
        return result
    def constant(value):
        raise ValueError('Non-finite JSON value: ' + value)
    def number(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError('Non-finite JSON number')
        return parsed
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)


class CodexAgentClient:
    def __init__(self, options, scope, prompt, values, max_requests):
        self.settings = validate_options(options)
        self.artifact_store = (LocalObjectStore(**self.settings['artifact_store'])
                               if self.settings.get('artifact_store') else None)
        self.sandbox = 'workspace-write' if self.artifact_store else 'read-only'
        self.timeout = options.get('timeout_s', 600)
        self.scope, self.prompt, self.values = scope, prompt, values
        self.declaration = scope.declaration
        self.media = OperatorImages(self.declaration)
        self.budget = CharacterBudget(self.declaration.max_context_chars)
        self.observations = []
        self.process = None
        self.thread_id = self.turn_id = None
        self.completed = False
        self.sequence = 0
        self.recorded = {'events': [], 'stderr': '', 'content': '', 'usage': {},
                         'native_searches': 0, 'peak_rss_bytes': 0, 'peak_scratch_bytes': 0}
        if self.artifact_store:
            self.recorded['artifacts'] = []
        self.output_bytes = self.stderr_bytes = 0
        self.source = None
        self.journal = (SQLitePromptJournal(**options['sqlite_journal'], max_requests=max_requests)
                        if options.get('sqlite_journal') else None)

    def state(self, observations=None):
        observations = self.observations if observations is None else observations
        return {'runtime': 'codex', 'resources': self.scope.resources,
                **self.remaining(len(observations)),
                'limits': {**{key: getattr(self.declaration, key) for key in LIMITS
                             if key not in ('max_turns', 'max_calls_per_turn')},
                           'max_operator_calls': self.declaration.max_operator_calls,
                           'session_timeout_s': self.timeout},
                'native_web_search': self.settings['web_search'], 'observations': observations}

    def remaining(self, used):
        return {'used_operator_calls': used,
                'remaining_operator_calls': self.declaration.max_operator_calls - used}

    def tools(self):
        return [{'type': 'function', 'name': tool['name'], 'description': tool['description'],
                 'inputSchema': tool['parameters'], 'deferLoading': False}
                for tool in self.declaration.tool_definitions()]

    def validate_input(self, request):
        # Image bytes are already bound by the shared template renderer. Check
        # their encoded size before making additional base64/message copies.
        estimate = 0
        for part in request.parts:
            if hasattr(part, 'image'):
                image = part.image
                estimate += len(image.uri) if image.uri else 4 * ((len(image.data or b'') + 2) // 3) + 128
            else:
                estimate += len(part.text.encode('utf-8'))
            if estimate > self.settings['max_input_bytes']:
                raise PromptBudgetExceededError('Codex input exceeds max_input_bytes')

    def prepare(self, request):
        self.validate_input(request)
        messages = request_messages(request)
        # Disabled image features preserve the existing text-only request
        # identity. Enabling either feature must invalidate that cached result.
        settings = {key: value for key, value in self.settings.items()
                    if key not in ('image_generation', 'view_image') or value is not False}
        self.source = {'transport': 'codex_app_server/1', 'stage': request.prompt_name,
            'prompt_version': request.prompt_version, 'model': request.model,
            'messages': messages, 'response_schema': dict(request.response_schema),
            'tools': self.tools(), 'environment': self.declaration.identity(),
            'resources': self.scope.resources, 'settings': settings, 'timeout_s': self.timeout}
        if self.artifact_store:
            self.source['artifact_protocol'] = 'codex-files/2'
        _encode(self.source, self.settings['max_input_bytes'])
        return request

    async def prepare_lookup(self, request):
        await _journal_io(self.prepare, request)
        saved = await _journal_io(self.journal.lookup, self.source) if self.journal else None
        if saved is None:
            return request, None
        try:
            # Replay policy belongs to the API. Never repeat paid/side-effect
            # calls just to reconstruct a completed Codex session.
            for observation in saved.get('observations', []):
                call = observation['call']
                if self.scope.replay_policy(call) == 'verify':
                    await self.invoke({'threadId': None, 'turnId': None,
                                       'tool': call['method'], 'arguments': call['arguments'],
                                       'namespace': observation.get('namespace')})
                else:
                    try:
                        await self.media.verify(observation.get('images', []))
                    except (OSError, ValueError, KeyError, TypeError) as exc:
                        raise PromptResponseContractError('Cached operator image delivery failed: ' + str(exc)) from exc
                    self.observations.append(observation)
            if canonical(self.observations) != canonical(saved.get('observations', [])):
                raise PromptResponseContractError('Codex cached operator observations changed; no session started')
        except Exception as exc:
            exc.call = self.metadata(saved, reused=True)
            raise
        return request, await _journal_io(self.decode, saved, True)

    def lookup(self, request):
        saved = self.journal.lookup(self.source) if self.journal else None
        return self.decode(saved, reused=True) if saved is not None else None

    def metadata(self, saved, reused=False):
        usage = saved.get('usage', {})
        metadata = {'transport': 'codex_app_server', 'runtime': 'codex',
            'model': self.prompt.model.name, 'model_source': 'requested',
            'web_search': self.settings['web_search'], 'reused': reused,
            'image_generation': self.settings['image_generation'],
            'elapsed_s': saved.get('elapsed_s', 0),
            'usage': {'input_tokens': usage.get('inputTokens', 0),
                      'output_tokens': usage.get('outputTokens', 0)} if usage else {},
            'codex_usage': usage,
            'native_searches': saved.get('native_searches', 0),
            'budget_unit': 'codex_session', 'execution_status': saved.get('status'),
            'peak_rss_bytes': saved.get('peak_rss_bytes', 0),
            'peak_scratch_bytes': saved.get('peak_scratch_bytes', 0)}
        if self.artifact_store:
            metadata.update(artifacts=saved.get('artifacts', []), artifact_store=self.settings['artifact_store'])
        if saved.get('artifact_error'):
            metadata['artifact_error'] = saved['artifact_error']
        if self.journal:
            metadata.update(self.journal.references(self.source))
        metadata['environment'] = {'protocol': 'demiflow.codex_environment.v1',
            'runtime': 'codex', 'turns': [dict(metadata)],
            'observations': saved.get('observations', self.observations)}
        return metadata

    def decode(self, saved, reused=False):
        metadata = self.metadata(saved, reused)
        try:
            if saved.get('status') != 'completed':
                raise ValueError('Codex session did not complete: ' + str(saved.get('status')))
            if reused and self.artifact_store:
                verify_artifacts(saved.get('artifacts', []), max_files=self.settings['max_artifact_files'],
                                 max_bytes=self.settings['max_artifact_bytes'])
        except Exception as exc:
            error = PromptResponseContractError('Codex response/artifact delivery failed: ' + str(exc))
            error.call = metadata
            raise error from exc
        usage = saved.get('usage', {})
        return OperatorLLMResponse(saved['content'], OperatorLLMRequestUsage(
            input_tokens=usage.get('inputTokens', 0), output_tokens=usage.get('outputTokens', 0)),
            metadata=metadata)

    async def invoke(self, params):
        if params.get('threadId') != self.thread_id or params.get('turnId') != self.turn_id:
            raise PromptResponseContractError('Codex callback belongs to another row/turn')
        if len(self.observations) >= self.declaration.max_operator_calls:
            raise PromptBudgetExceededError('Codex max_operator_calls exhausted; no operator executed')
        call = {'method': params.get('tool'), 'arguments': params.get('arguments')}
        identity = {'call': call}
        if params.get('namespace'):
            identity['namespace'] = params['namespace']
        # PromptContext crosses the native reading worker boundary. Capture
        # only immutable row data, never this client's event loop or process.
        base, state = self.values, self.state()
        include_state = STATE in self.prompt.template.arguments
        remaining = self.remaining(len(self.observations) + 1)
        def inputs(row, reading):
            observed = [*state['observations'], {**identity, 'result': reading}]
            return ({**base, STATE: {**state, **remaining, 'observations': observed}}
                    if include_state else base)
        context = PromptContext(self.prompt, self.budget, inputs)
        try:
            if params.get('namespace') not in (None, ''):
                raise OperatorCallError('operator_not_allowed', 'Unknown operator namespace')
            result = await self.scope.invoke(call, context=context)
        except OperatorCallError as exc:
            result = _error(exc.code, str(exc))
        except (OSError, TimeoutError) as exc:
            result = _error('execution_error', str(exc))
        observation = {**identity, 'result': result}
        content = []
        success = not isinstance(result, dict) or result.get('status') != 'error'
        if success:
            receipts, attached = await self.media.render(result, self.declaration.image_output(call['method']))
            if receipts:
                observation['images'] = receipts
                pictures = iter(attached)
                for receipt in receipts:
                    if receipt['status'] == 'attached':
                        content.extend([{'type': 'inputText', 'text': 'Image ' + receipt['image_id']},
                                        {'type': 'inputImage', 'imageUrl': next(pictures)}])
        if len(canonical(observation)) > self.declaration.max_observation_chars:
            raise PromptBudgetExceededError('Operator observation exceeds max_observation_chars')
        current = ({**base, STATE: self.state([*self.observations, observation])} if include_state else base)
        context_chars = self.budget.counter.prompt(self.prompt, current)
        if not include_state:
            context_chars += len(canonical([*self.observations, observation]))
        if context_chars > self.budget.max_input:
            raise PromptBudgetExceededError('Demiflow supplied context exceeds max_context_chars')
        self.observations.append(observation)
        body = {'result': result, 'environment': self.remaining(len(self.observations))}
        if observation.get('images'):
            body['images'] = observation['images']
        return {'success': success,
                'contentItems': [{'type': 'inputText', 'text': canonical(body)}, *content]}

    async def send(self, value):
        encoded = _encode(value, self.settings['max_input_bytes'])
        try:
            self.process.stdin.write(encoded + b'\n')
            await self.process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            # A lost model transport is a failed row, not a storage failure.
            # Preserve the journal/partial response and let error_output handle it.
            raise PromptResponseContractError('Codex app-server connection lost while sending a message') from exc

    async def receive(self):
        message_limit = self.settings['max_message_bytes']
        # null disables the separate event cap, while the session stdout budget
        # still bounds buffering before JSON decoding.
        limit_name = 'max_message_bytes' if message_limit is not None else 'max_output_bytes'
        try:
            raw = await self.process.stdout.readline()
        except ValueError as exc:
            raise PromptBudgetExceededError('Codex event exceeds ' + limit_name) from exc
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise PromptResponseContractError('Codex app-server connection lost while reading a message') from exc
        if not raw:
            raise PromptResponseContractError('Codex app-server closed before completing the response')
        if message_limit is not None and len(raw) > message_limit:
            raise PromptBudgetExceededError('Codex event exceeds max_message_bytes')
        self.output_bytes += len(raw)
        if self.output_bytes > self.settings['max_output_bytes']:
            raise PromptBudgetExceededError('Codex stdout exceeds max_output_bytes')
        if len(self.recorded['events']) >= self.settings['max_events']:
            raise PromptBudgetExceededError('Codex event count exceeds max_events')
        try:
            event = _strict_json(raw)
        except (ValueError, RecursionError) as exc:
            raise PromptResponseContractError('Codex app-server emitted invalid JSON') from exc
        if not isinstance(event, dict):
            raise PromptResponseContractError('Codex app-server event must be an object')
        self.recorded['events'].append(_metadata(event))
        return event

    async def dispatch(self, event):
        method, params = event.get('method'), event.get('params', {})
        if 'id' in event:
            if method != 'item/tool/call':
                await self.send({'id': event['id'], 'error': {'code': -32601,
                    'message': 'Unsupported request; no interactive approval or arbitrary callback'}})
                raise PromptResponseContractError('Unsupported Codex server request: ' + str(method))
            await self.send({'id': event['id'], 'result': await self.invoke(params)})
            return
        if params.get('threadId') is not None and self.thread_id is not None:
            if params['threadId'] != self.thread_id:
                raise PromptResponseContractError('Codex event belongs to another row')
        if method == 'turn/started':
            self.turn_id = params['turn']['id']
        elif method == 'thread/tokenUsage/updated':
            self.recorded['usage'] = params['tokenUsage']['total']
        elif method == 'item/started' and params.get('item', {}).get('type') == 'webSearch':
            self.recorded['native_searches'] += 1
        elif method == 'item/completed':
            self.accept_item(params.get('item', {}))
        elif method == 'turn/completed':
            turn = params['turn']
            if turn['id'] != self.turn_id or turn['status'] != 'completed':
                raise PromptResponseContractError('Codex turn failed or interrupted: ' + canonical(turn.get('error')))
            for item in turn.get('items', []):
                self.accept_item(item)
            if not self.recorded['content']:
                raise PromptResponseContractError('Codex completed without a final answer')
            self.completed = True

    def accept_item(self, item):
        if item.get('type') == 'agentMessage' and item.get('phase') in (None, 'final_answer'):
            text = item.get('text', '')
            if not isinstance(text, str):
                raise PromptResponseContractError('Codex final answer must be text')
            if len(text) > self.declaration.max_response_chars:
                raise PromptBudgetExceededError('Codex final answer exceeds max_response_chars')
            self.recorded['content'] = text

    async def rpc(self, method, params):
        self.sequence += 1
        identity = self.sequence
        await self.send({'id': identity, 'method': method, 'params': params})
        while True:
            event = await self.receive()
            if 'method' not in event and event.get('id') == identity:
                if 'error' in event:
                    raise PromptResponseContractError('Codex ' + method + ' failed: ' + canonical(event['error']))
                return event['result']
            await self.dispatch(event)

    async def session(self, directory):
        await self.rpc('initialize', {'clientInfo': {'name': 'demiflow', 'version': '1'},
                                      'capabilities': {'experimentalApi': True}})
        await self.send({'method': 'initialized'})
        started = await self.rpc('thread/start', {'model': self.prompt.model.name,
            'cwd': directory, 'ephemeral': True, 'approvalPolicy': 'never', 'sandbox': self.sandbox,
            'dynamicTools': self.tools()})
        self.thread_id = started['thread']['id']
        if started.get('model') != self.prompt.model.name:
            raise PromptResponseContractError('Codex selected a model different from the requested model')
        inputs = []
        for message in self.source['messages']:
            inputs.append({'type': 'text', 'text': message['role'].upper() + ':'})
            parts = message['content']
            for part in ([{'type': 'text', 'text': parts}] if isinstance(parts, str) else parts):
                if part['type'] == 'text':
                    inputs.append({'type': 'text', 'text': part['text']})
                elif part['type'] == 'image_url':
                    url = part['image_url']['url']
                    if not url.startswith('data:image/'):
                        raise ValueError('Codex images must be bound inline image data URLs')
                    inputs.append({'type': 'image', 'url': url})
                else:
                    raise ValueError('Unsupported Codex input part')
        if self.artifact_store:
            context = await _journal_io(partial(file_context, self.source['messages'], directory,
                max_files=self.settings['max_artifact_files'], max_bytes=self.settings['max_artifact_bytes']))
            self.recorded['file_context'] = context
            inputs.append({'type': 'text', 'text': context_message(context)})
        params = {'threadId': self.thread_id, 'input': inputs,
                  'outputSchema': self.settings.get('output_schema', self.source['response_schema'])}
        if self.settings.get('reasoning_effort'):
            params['effort'] = self.settings['reasoning_effort']
        response = await self.rpc('turn/start', params)
        if self.turn_id is not None and self.turn_id != response['turn']['id']:
            raise PromptResponseContractError('Codex changed turn identity')
        self.turn_id = response['turn']['id']
        while not self.completed:
            await self.dispatch(await self.receive())
        # The original schema is also validated by the shared runtime. Strict
        # JSON checks here prevent duplicate keys/non-finite values being lost.
        self.recorded['content'] = _strict_json(self.recorded['content'])

    async def stderr(self):
        while True:
            remaining = self.settings['max_stderr_bytes'] - self.stderr_bytes
            chunk = await self.process.stderr.read(min(8192, remaining + 1))
            if not chunk:
                return
            self.stderr_bytes += len(chunk)
            if self.stderr_bytes > self.settings['max_stderr_bytes']:
                raise PromptBudgetExceededError('Codex stderr exceeds max_stderr_bytes')
            self.recorded['stderr'] += chunk.decode('utf-8', errors='replace')

    async def guard(self, directory):
        import psutil
        root = psutil.Process(self.process.pid)
        while True:
            try:
                # Subagents are disabled. Include any native tool children
                # in sampled RSS protection; this is not a kernel RSS ceiling.
                children = root.children(recursive=True)
                if len(children) > 32:
                    raise PromptBudgetExceededError('Codex process count exceeds 32')
                rss = sum(p.memory_info().rss for p in [root, *children] if p.is_running())
                self.recorded['peak_rss_bytes'] = max(self.recorded['peak_rss_bytes'], rss)
                if rss > self.settings['max_rss_bytes']:
                    raise PromptBudgetExceededError('Codex sampled RSS exceeds max_rss_bytes')
            except psutil.NoSuchProcess:
                return
            def scratch_size():
                size = count = 0
                for base, dirs, files in os.walk(directory, followlinks=False):
                    count += len(dirs) + len(files)
                    if count > 1024:
                        raise PromptBudgetExceededError('Codex scratch entry count exceeds 1024')
                    for name in files:
                        try:
                            size += os.lstat(os.path.join(base, name)).st_size
                        except FileNotFoundError:
                            pass
                        if size > self.settings['max_scratch_bytes']:
                            raise PromptBudgetExceededError('Codex scratch exceeds max_scratch_bytes')
                return size
            size = await _journal_io(scratch_size)
            self.recorded['peak_scratch_bytes'] = max(self.recorded['peak_scratch_bytes'], size)
            await asyncio.sleep(.1)

    def kill(self):
        if self.process is not None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    async def stop(self):
        if self.process is None:
            return
        # Kill the group even when the leader already exited. No other row's
        # process is touched. SIGKILL also closes inherited pipe descriptors.
        self.kill()
        process, self.process = self.process, None
        async def drain(stream):
            while await stream.read(8192):
                pass
        process.stdin.close()
        await asyncio.gather(drain(process.stdout), drain(process.stderr))
        await process.wait()

    async def execute(self, request):
        if self.journal and not await _journal_io(self.journal.reserve, self.source):
            _, response = await self.prepare_lookup(request)
            return response
        started = time.monotonic()
        response_saved = False
        try:
            executable = shutil.which(self.settings.get('bin') or 'codex')
            if not executable:
                raise ValueError('Codex binary not found; configure codex_agent.bin')
            with tempfile.TemporaryDirectory(prefix='demiflow-codex-agent-') as directory:
                config = {'approval_policy': 'never', 'sandbox_mode': self.sandbox,
                    'project_doc_max_bytes': 0, 'web_search': self.settings['web_search'],
                    'mcp_servers': {}, 'plugins': {}, 'agents.enabled': False,
                    'features.apps': False, 'features.hooks': False,
                    'features.shell_tool': self.settings['shell_tool'],
                    'features.multi_agent': False, 'features.multi_agent_v2': False,
                    'features.image_generation': self.settings['image_generation'],
                    'features.view_image': self.settings['view_image'],
                    'features.skill_search': False, 'features.skip_host_skill_discovery': True,
                    'log_dir': directory, 'sqlite_home': directory, 'history.persistence': 'none'}
                if self.artifact_store:
                    config.update({'sandbox_workspace_write.exclude_slash_tmp': True,
                                   'sandbox_workspace_write.exclude_tmpdir_env_var': True,
                                   'sandbox_workspace_write.writable_roots': [],
                                   'sandbox_workspace_write.network_access': False})
                command = [executable, 'app-server', '--listen', 'stdio://']
                for name, value in config.items():
                    command += ['-c', name + '=' + ('{}' if value == {} else json.dumps(value))]
                launcher = str(Path(__file__).with_name('codex_agent_worker.py'))
                self.process = await asyncio.create_subprocess_exec(
                    sys.executable, launcher, str(self.settings['max_scratch_bytes']),
                    str(math.ceil(self.timeout)), *command, cwd=directory,
                    env={**os.environ, 'TMPDIR': directory, 'TOKIO_WORKER_THREADS': '2', 'RAYON_NUM_THREADS': '2'},
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    limit=self.settings['max_message_bytes'] or self.settings['max_output_bytes'],
                    start_new_session=True)
                session = asyncio.create_task(self.session(directory))
                readers = [asyncio.create_task(self.stderr()), asyncio.create_task(self.guard(directory))]
                try:
                    async with asyncio.timeout(self.timeout):
                        pending = {session, *readers}
                        while not session.done():
                            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                            for task in done:
                                task.result()
                        session.result()
                finally:
                    # Stop paid activity before waiting for native worker cleanup.
                    self.kill()
                    for task in [session, *readers]:
                        task.cancel()
                    await asyncio.gather(session, *readers, return_exceptions=True)
                    await asyncio.shield(self.stop())
                self.recorded['status'] = 'completed'
                if self.artifact_store:
                    try:
                        await _journal_io(partial(collect_artifacts, Path(directory) / 'artifacts', self.recorded,
                            store=self.artifact_store, max_files=self.settings['max_artifact_files'],
                            max_bytes=self.settings['max_artifact_bytes']))
                    except Exception as exc:
                        self.recorded.update(status='artifact_failed', artifact_error=type(exc).__name__ + ': ' + str(exc))
            self.recorded.update(elapsed_s=time.monotonic() - started,
                                 observations=self.observations)
            if self.journal:
                await _journal_io(self.journal.response, self.source, self.recorded)
            response_saved = True
            return self.decode(self.recorded)
        except BaseException as exc:
            if response_saved:
                raise
            self.recorded.update(status='failed', elapsed_s=time.monotonic() - started,
                                 observations=self.observations)
            exc.call = self.metadata(self.recorded)
            # Unfinished sessions stay uncertain; never silently start a second
            # potentially paid agent on resume. Preserve bounded diagnostic data.
            if self.journal:
                exc.call['partial_response'] = self.recorded
                await _journal_io(self.journal.failed, self.source, exc, self.recorded['elapsed_s'])
                exc.call.pop('partial_response', None)
            raise

    async def aclose(self):
        await self.stop()
        if self.journal:
            await _journal_io(self.journal.close)


async def call_in_codex_environment(runtime, prompt_name, values, resources, declaration):
    prompt = declaration.prompt(resolve_prompt(runtime.config, prompt_name))
    scope = declaration.bind(resources)
    client = CodexAgentClient(runtime.options or {}, scope, prompt, values,
                              runtime.coordinator.max_requests)
    current = {**values, STATE: client.state()} if STATE in prompt.template.arguments else values
    try:
        result, trace = await runtime.call_with_trace(prompt_name, current,
            prompt_override=prompt, input_budget=client.budget, client_override=client)
        return result, trace
    finally:
        try:
            await client.aclose()
        finally:
            await scope.aclose()
