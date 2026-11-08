"""Assemble authoritative context and invoke only the public development CLI.

CLI read receipts and delivery to a provider are separate facts. Preparation may
write a CLI receipt; no model turn is dispatched until its context is acknowledged.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import time

from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import text
from demiflow.workbench.conversations.common import wire

DEVELOPMENT = Path(__file__).resolve().parents[2] / 'forge/development'
LAUNCHER = DEVELOPMENT / 'skill/scripts/forge.py'


def cli(root, arguments, *, scratch, timeout=30, maximum=1024 * 1024):
    """Fixed trusted launcher, isolated imports, bounded stdout and stderr."""
    env = dict(PATH='/usr/bin:/bin', HOME=str(scratch), TMPDIR=str(scratch), LANG='C.UTF-8',
               PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1')
    command = [sys.executable, '-I', '-B', str(LAUNCHER), '--project', str(root), *arguments]
    command = [sys.executable, '-I', '-B', str(Path(__file__).with_name('provider_exec.py')), str(os.getpid()), *command]
    process = subprocess.Popen(command, cwd=DEVELOPMENT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = {'stdout': bytearray(), 'stderr': bytearray()}
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + timeout
    try:
        for name, stream in (('stdout', process.stdout), ('stderr', process.stderr)):
            os.set_blocking(stream.fileno(), False); selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map():
            if time.monotonic() > deadline:
                reject('Development CLI exceeded its time budget', 'resource_limit')
            for key, _ in selector.select(0.1):
                chunk = os.read(key.fd, min(65536, maximum + 1 - len(output[key.data])))
                if not chunk:
                    selector.unregister(key.fileobj); continue
                if len(output[key.data]) + len(chunk) > maximum:
                    reject('Development CLI output exceeded budget', 'resource_limit')
                output[key.data].extend(chunk)
        process.wait(timeout=max(0.1, deadline - time.monotonic()))
    finally:
        if process.poll() is None:
            process.kill(); process.wait(timeout=2)
        selector.close(); process.stdout.close(); process.stderr.close()
    if process.returncode not in {0, 1}:
        reject('Development CLI rejected the operation: ' + output['stderr'].decode(errors='replace')[:2048])
    raw = output['stdout'].decode('utf-8')
    try:
        value = json.loads(raw)
    except ValueError:
        value = raw
    return value


def assemble(project, requirement, scratch, limits):
    root = Path(project['root'])
    sources, pages, used = [], [], 0

    def append(source_id, path, raw, *, binding=None, page=1, page_count=1, selection=None, sha=None):
        nonlocal used
        used += len(raw.encode())
        if used > limits.context_bytes or len(pages) >= 32:
            reject('Required context exceeds initial byte/page budget; split the task explicitly', 'resource_limit')
        sources.append(dict(kind=project['kind'], source_id=source_id, canonical_path=str(path),
            sha256=sha or hashlib.sha256(raw.encode()).hexdigest(), version=binding, required=True,
            selection=selection, page=page, pages=page_count, bytes=len(raw.encode()),
            delivery='prepared', origin='public_development_cli' if binding else 'authoritative_file'))
        pages.append(raw)

    paths = []
    # Ancestor AGENTS apply to the actual root; scoped descendants apply to writes.
    for parent in reversed((root, *root.parents)):
        candidate = parent / 'AGENTS.md'
        if candidate.is_file(): paths.append(candidate)
        # Business project guidance applies to the original root, including the
        # inherited pipeline specification; never copy or truncate that source.
        specification = parent / 'PIPELINE_SPEC.md'
        if project['kind'] != 'platform' and specification.is_file():
            paths.append(specification)
    for relative in project['writes']:
        candidate = safe_path(root, relative, missing=True)
        if not candidate.is_dir(): candidate = candidate.parent
        while candidate != root and root in candidate.parents:
            agents = candidate / 'AGENTS.md'
            if agents.is_file() and agents not in paths: paths.append(agents)
            candidate = candidate.parent
    paths.extend(safe_path(root, p) for p in project['sources'])
    if project['kind'] == 'native_forge':
        paths.append(safe_path(root, 'workspace.json'))
    for path in dict.fromkeys(paths):
        raw = read_file(path).decode('utf-8')
        append(str(path), path, '# Source: ' + str(path) + '\n' + raw,
               sha=hashlib.sha256(raw.encode()).hexdigest())
    binding = None
    if project['kind'] == 'demiforge_registered':
        task = project['task_id']
        cli(root, ['list', '--limit', '20'], scratch=scratch)
        exists = safe_path(root, '.demiforge/tasks/' + task + '/task.json', missing=True).exists()
        history = cli(root, ['history', '--id', task], scratch=scratch) if exists else None
        # History is deliberately read before current binding; a stale task fails.
        if not exists:
            if project['phase'] != 'implementation':
                reject('No registered task: implementation scope must be explicitly granted')
            args = ['start', '--id', task, '--pipeline', project['pipeline'], '--goal', requirement]
            for path in project['writes']: args += ['--write', path]
            for goal in project['non_goals']: args += ['--non-goal', goal]
            cli(root, args, scratch=scratch)
        binding = cli(root, ['context', '--id', task], scratch=scratch)
        if not isinstance(binding, dict) or binding.get('writes') != project['writes']:
            reject('Registered task writable scope differs from the deployment grant', 'conflict')
        append('canonical-skill', DEVELOPMENT / 'skill/SKILL.md', read_file(DEVELOPMENT / 'skill/SKILL.md').decode())
        for key, info in binding['sources'].items():
            for number in range(1, info['pages'] + 1):
                raw = cli(root, ['context', '--id', task, '--read', key, '--page', str(number)], scratch=scratch)
                if not isinstance(raw, str): reject('Invalid public CLI source page')
                definition = info['definition']
                path = (root / definition['path']).resolve()
                append(key, path, raw, binding=info['sha256'], sha=info['sha256'], page=number,
                       page_count=info['pages'], selection=definition.get('lines'))
    grant = {key: project.get(key) for key in ('kind', 'phase', 'writes', 'watch', 'non_goals', 'authorization', 'task_id', 'design_review')}
    header = (
        '你正在 DemiFlow 中执行一个明确授权的项目开发任务。\n'
        '实际项目目录：' + str(root) + '\n授权与范围：' + wire(grant).decode() + '\n'
        '这是 trusted_local 部署：宿主权限可用，没有 OS 文件或进程沙箱。只在上述范围内工作；'
        '不得读取凭证、修改共享环境、启动生产任务、联网工具、创建子代理/日程或联系其他会话。'
        '不要改写控制目录或旧任务基线。保留 dirty 内容与 Notebook 输出。'
        '设计阶段仅可写明确列出的设计文档。遇到范围冲突或规范缺失须说明，不能擅自放宽。'
        '你的文字与命令输出不代表检查、独立 Review、Freeze 或验收通过。'
        '使用项目现有验证入口；开发桥只调用公开 CLI。内部模型调用次数和费用未封顶。\n'
        '当前真人要求（原文）：\n' + requirement + '\n权威来源正文：\n'
    )
    manifest = dict(kind=project['kind'], task_id=project.get('task_id'), grant=grant,
                    requirement=requirement, sources=sources, bytes=used, evidence='missing', binding=binding)
    prompt = header + '\n\n'.join(pages)
    if len(prompt.encode()) > limits.context_bytes:
        reject('Context with trusted header exceeds initial budget', 'resource_limit')
    manifest['digest'] = digest(manifest)
    return manifest, prompt


BRIDGE = dict(name='development_protocol', description='Use the existing registered project development CLI. No arbitrary commands or independent acceptance.',
    inputSchema=dict(type='object', additionalProperties=False, required=['operation', 'arguments'], properties={
        'operation': dict(type='string', enum=['context', 'context_read', 'ack_context', 'note', 'refresh', 'check', 'verify', 'self_review', 'handoff']),
        'arguments': dict(type='object')}))


def bridge(project, request, scratch, *, delivery):
    if project['kind'] != 'demiforge_registered':
        reject('This project has no registered development CLI task')
    if delivery != 'acknowledged' and request.get('operation') not in {'context', 'context_read', 'refresh', 'note'}:
        reject('Current context delivery is not acknowledged')
    if type(request) is not dict or set(request) != {'operation', 'arguments'} or type(request['arguments']) is not dict:
        reject('Invalid development bridge request')
    operation, args = request['operation'], request['arguments']
    allowed = {'context': set(), 'context_read': {'source', 'page'}, 'note': {'kind', 'text'}, 'refresh': {'reason'}, 'check': set(),
               'verify': set(), 'self_review': {'review'}, 'handoff': set()}
    if operation not in allowed or set(args) != allowed[operation]: reject('Unknown development bridge arguments')
    if project['phase'] == 'design' and operation not in {'context', 'note'}:
        reject('Operation requires implementation phase')
    command = ['review' if operation == 'self_review' else 'context' if operation == 'context_read' else operation, '--id', project['task_id']]
    if operation == 'context_read':
        from demiflow.workbench.conversations.common import integer
        command += ['--read', text(args['source'], 128), '--page', str(integer(args['page'], 1, 128))]
        return cli(project['root'], command, scratch=scratch, maximum=65536)
    if operation == 'verify':
        if project.get('allow_tests') is not True: reject('Trusted fixture execution was not granted')
        command += ['--allow-tests']
    elif operation == 'self_review':
        path = Path(scratch) / 'self-review.json'
        path.write_bytes(wire(args['review'], 32768))
        command += ['--file', str(path), '--reviewer', 'development-chat:self']
    else:
        for key, value in args.items(): command += ['--' + key, text(value, 8192)]
    return cli(project['root'], command, scratch=scratch, timeout=60, maximum=65536)


class ProtocolBridge:
    """Per-connection page delivery state shared by Codex and the ACP MCP bridge."""
    def __init__(self, store, tid, project, scratch):
        self.store, self.tid, self.project, self.scratch = store, tid, project, scratch
        manifest = json.loads(store.turn(tid)['context'] or '{}')
        self.used = manifest.get('bytes', 0)
        self.pages, self.required, self.seen = {}, set(), set()
        self.sent_page = None

    def invoke(self, request):
        if type(request) is not dict or set(request) != {'operation', 'arguments'} or type(request['arguments']) is not dict:
            reject('Invalid development bridge request')
        operation, args = request['operation'], request['arguments']
        self.sent_page = None
        if operation == 'ack_context':
            if set(args) != {'digest'} or args['digest'] not in self.pages:
                reject('Unknown delivered context page digest')
            record = self.pages.pop(args['digest'])
            self.seen.add((record['source'], record['page']))
            self.store.event(self.tid, 'context.page.acknowledged', record)
            if self.required and self.seen >= self.required:
                self.store.update(self.tid, delivery='acknowledged')
            return {'delivery': self.store.turn(self.tid)['delivery']}
        answer = bridge(self.project, request, self.scratch, delivery=self.store.turn(self.tid)['delivery'])
        if operation in {'check', 'verify', 'self_review', 'handoff'}:
            summary = {key: answer.get(key) for key in ('status', 'passed', 'fingerprint', 'ready') if isinstance(answer, dict) and key in answer}
            self.store.event(self.tid, 'check.reference', dict(operation=operation, task_id=self.project['task_id'],
                source='public_development_cli', independent_review=False,
                review_kind='self' if operation == 'self_review' else None, result=summary))
        if operation == 'refresh':
            self.seen.clear(); self.pages.clear()
            listing = bridge(self.project, {'operation': 'context', 'arguments': {}}, self.scratch, delivery='stale')
            self.required = {(key, page) for key, info in listing['sources'].items() for page in range(1, info['pages'] + 1)}
            if len(self.required) > 32: reject('Refreshed context exceeds page budget')
            self.store.update(self.tid, delivery='stale')
        if operation == 'context_read':
            size = len(answer.encode())
            if self.used + size > self.store.limits.context_total or len(self.pages) >= 32:
                reject('Supplemental context exceeds turn budget', 'resource_limit')
            self.used += size
            record = dict(source=args['source'], page=args['page'], bytes=size, digest=digest(answer))
            self.pages[record['digest']] = record
            self.store.event(self.tid, 'context.page.prepared', record)
            self.sent_page = record
            answer = dict(text=answer, delivery_digest=record['digest'], next='Call ack_context with this digest before dependent editing/checking')
        return answer

    def sent(self):
        if self.sent_page:
            self.store.event(self.tid, 'context.page.sent', self.sent_page)
            self.sent_page = None
