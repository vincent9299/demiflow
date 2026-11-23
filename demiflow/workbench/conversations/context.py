"""Prepare current coding guidance and serve turn-bound read-only input."""
from __future__ import annotations

import hashlib
import hmac
import os
from pathlib import Path
import secrets
import socket
import sys
import tempfile
import threading
import time

from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import wire


def authoritative_source_paths(project):
    root = Path(project['root'])
    paths = []
    parents = reversed((root, *root.parents)) if project['kind'] == 'platform' else (root,)
    for parent in parents:
        candidate = parent / 'AGENTS.md'
        if candidate.is_file(): paths.append(candidate)
    for relative in project['writes']:
        candidate = safe_path(root, relative, missing=True)
        if not candidate.is_dir(): candidate = candidate.parent
        while candidate != root and root in candidate.parents:
            agents = candidate / 'AGENTS.md'
            if agents.is_file() and agents not in paths: paths.append(agents)
            candidate = candidate.parent
    paths.extend(safe_path(root, p) for p in project['sources'])
    return list(dict.fromkeys(paths))


def assemble(project, requirement, limits, *, execution_mode='trusted_local', input_access=False,
             coding=None, delivery_review=False, previous_review=None):
    root = Path(project['root'])
    sources, pages, used = [], [], 0

    def append(source_id, path, raw, *, binding=None, sha=None, origin="authoritative_file"):
        nonlocal used
        used += len(raw.encode())
        if used > limits.context_bytes or len(pages) >= 32:
            reject('Required context exceeds initial byte/page budget; split the task explicitly', 'resource_limit')
        sources.append(dict(kind=project['kind'], source_id=source_id, canonical_path=str(path),
            sha256=sha or hashlib.sha256(raw.encode()).hexdigest(), version=binding, required=True,
            bytes=len(raw.encode()), delivery='prepared', origin=origin))
        pages.append(raw)

    for path in authoritative_source_paths(project):
        raw = read_file(path).decode('utf-8')
        append(str(path), path, '# Source: ' + str(path) + '\n' + raw,
               sha=hashlib.sha256(raw.encode()).hexdigest())
    guidance = None
    if coding is not None:
        from importlib.resources import files as resource_files
        from demiflow.forge.context import build_context

        facts = build_context(root, reference=coding, exclude=project.get('protected', ()))
        if facts['status'] != 'ready':
            reject('Forge coding context incomplete: ' + '; '.join(
                item['detail'] for item in facts['diagnostics']), 'resource_limit')
        sdk = facts['sdk']
        guidance = dict(sdk_revision=sdk['sdk_revision'], catalog_id=sdk['catalog_id'],
                        rules_id=facts['rules_id'], symbols=[item['id'] for item in sdk['apis']],
                        sources=[{key: item[key] for key in ('origin', 'path', 'sha256')}
                                 for item in facts['sources']])
        skill_text = resource_files('demiflow.forge').joinpath('skills', 'demiforge', 'SKILL.md').read_text(encoding='utf-8')
        append('forge-skill', root, skill_text, binding=digest(facts),
               origin='forge_skill')
        if previous_review:
            append('forge-pipeline-review', root,
                   '本会话上次独立评审，当时的源码及报告仅供本轮复核：\n'
                   + wire(previous_review).decode(), origin='forge_review_result')
    grant = {key: project[key] for key in ('kind', 'phase', 'writes', 'watch', 'non_goals', 'authorization')}
    manifest = dict(kind=project['kind'], grant=grant, requirement=requirement, sources=sources, bytes=used)
    if guidance is not None:
        manifest['coding_guidance'] = guidance
    if input_access: manifest['input_access'] = 'conversation_readonly'
    # Strict sandbox enforces boundaries in the OS; domain guidance lives solely in
    # the attached Forge Skill (and any explicitly declared project sources).
    prompt = '\n\n'.join(pages) if pages else ('实际项目目录：' + str(root) + '\n')
    if len(prompt.encode()) > limits.context_bytes:
        reject('Context with trusted header exceeds initial budget', 'resource_limit')
    manifest['digest'] = digest(manifest)
    return manifest, prompt


INPUT_TOOL = dict(name='read_input', description='Discover and read bounded input data.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'action': dict(type='string', enum=['list', 'find', 'read', 'inspect']),
        'path': dict(type='string'), 'pattern': dict(type='string'),
        'depth': dict(type='integer'), 'offset': dict(type='integer'), 'bytes': dict(type='integer'),
        'limit': dict(type='integer'), 'columns': dict(type='array', items=dict(type='string'))}))


class BridgeServer:
    """Bounded input IPC owned by the trusted worker.

    The MCP subprocess receives only an endpoint and one-turn capability token.
    B1 may relay this Unix endpoint into its isolated network namespace; the same
    token, epoch, active-turn and fixed-operation checks apply on this side.
    """
    def __init__(self, input_access, *, delivery_review=None):
        self.lock = threading.Lock()
        self.input_access = input_access
        self.delivery_review = delivery_review
        self.bound_identity = (input_access.tid, input_access.epoch) if input_access is not None else None
        self.token = secrets.token_urlsafe(32)
        self.directory = Path(tempfile.mkdtemp(prefix='demiflow-dev-control-'))
        self.path = self.directory / 'bridge.sock'
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(self.path)); os.chmod(self.path, 0o600)
        self.socket.listen(4); self.socket.settimeout(.2)
        self.stopping = threading.Event()
        self.count = 0
        self.thread = threading.Thread(target=self._run, name='development-control', daemon=True)
        self.thread.start()

    def set_turn(self, input_access, *, delivery_review=None):
        with self.lock:
            self.input_access = input_access
            self.delivery_review = delivery_review
            self.count = 0
            if input_access is not None and self.bound_identity is None:
                self.bound_identity = (input_access.tid, input_access.epoch)

    def _answer(self, request):
        with self.lock:
            active = self.input_access
            if (active is None or type(request) is not dict or set(request) != {'token', 'turn_id', 'epoch', 'method', 'params'}
                    or type(request['token']) is not str or not hmac.compare_digest(request['token'], self.token)
                    or (request['turn_id'], request['epoch']) not in {(active.tid, active.epoch), self.bound_identity}):
                reject('Development control capability does not match this turn', 'not_found')
            active.require_active()
        return self.dispatch(request['method'], request['params'], active)

    def dispatch(self, method, params, active):
        from .pipeline_review import REVIEW_TOOL
        with self.lock:
            if active is not self.input_access:
                reject('Development control capability does not match this turn', 'not_found')
            if method == 'tools/list' and params == {}:
                return dict(tools=[INPUT_TOOL, REVIEW_TOOL] if self.delivery_review is not None else [INPUT_TOOL])
            if method != 'tools/call' or type(params) is not dict or set(params) != {'name', 'arguments'}:
                reject('Unsupported development control operation')
            if params['name'] == REVIEW_TOOL['name'] and self.delivery_review is not None:
                # End-of-turn detachment serializes with this short durable admission.
                answer = self.delivery_review.request(params['arguments'])
            elif params['name'] != INPUT_TOOL['name']:
                reject('Unsupported development control operation')
        if params['name'] == INPUT_TOOL['name']:
            answer = active.invoke(params['arguments'])
        return dict(content=[dict(type='text', text=wire(answer, 60000).decode())], isError=False)

    def _run(self):
        from demiflow.workbench.conversations.transport import decode
        while not self.stopping.is_set():
            try: connection, _ = self.socket.accept()
            except socket.timeout: continue
            except OSError: return
            with connection:
                connection.settimeout(2)
                try:
                    with self.lock:
                        if self.count >= 256:
                            reject('Development control request budget exceeded', 'resource_limit')
                        self.count += 1
                    with connection.makefile('rb') as stream:
                        raw = stream.readline(65537)
                    if len(raw) > 65536 or not raw.endswith(b'\n'): reject('Development control frame exceeds budget')
                    result = dict(result=self._answer(decode(raw)))
                except Exception as exc:
                    result = dict(error=dict(code=-32602, message=str(exc)[:2048]))
                try: connection.sendall(wire(result, 65536) + b'\n')
                except OSError: pass

    def close(self):
        self.stopping.set()
        self.socket.close()
        self.thread.join(timeout=3)
        try: self.path.unlink()
        except FileNotFoundError: pass
        try: self.directory.rmdir()
        except OSError: pass
