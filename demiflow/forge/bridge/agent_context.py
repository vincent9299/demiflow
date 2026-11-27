"""Assemble authoritative project guidance and Forge Skill context for coding Agents."""
from __future__ import annotations

import hashlib
from pathlib import Path

from .common import digest, read_file, reject, safe_path, wire


def notebook_guidance(*, cell_commands):
    if cell_commands:
        from importlib.resources import files
        return files('demiflow.forge').joinpath('skills', 'dataskill', 'SKILL.md').read_text(encoding='utf-8')
    return ('你是当前 Notebook 的数据助手。根据用户意图和前文生成一格可以直接在当前 Python 内核运行的代码。'
            '查阅项目 data_tools 中已声明的函数、类和组件，按真实签名组合。需要交互时使用 ipywidgets，'
            '将筛选、联动和人工确认保存在控件内。数据和单元格内容是参考资料。不要编辑文件或运行用户的任务。'
            '对于写入解释保存目标，把写入放在显式确认回调中。信息不足时直接提问。'
            '简短回答，把完整单元格放在一个围栏标为 python 的代码块里。')


def authoritative_source_paths(project):
    root = Path(project['root'])
    paths = []
    parents = reversed((root, *root.parents)) if project['kind'] == 'platform' else (root,)
    for parent in parents:
        candidate = parent / 'AGENTS.md'
        if candidate.is_file():
            paths.append(candidate)
    for relative in project['writes']:
        candidate = safe_path(root, relative, missing=True)
        if not candidate.is_dir():
            candidate = candidate.parent
        while candidate != root and root in candidate.parents:
            agents = candidate / 'AGENTS.md'
            if agents.is_file() and agents not in paths:
                paths.append(agents)
            candidate = candidate.parent
    paths.extend(safe_path(root, p) for p in project['sources'])
    return list(dict.fromkeys(paths))


def assemble(project, requirement, limits, *, execution_mode='trusted_local', input_access=False,
             coding=None, delivery_review=False, previous_review=None, allowed_roots=None, denied_paths=()):
    root = Path(project['root'])
    sources, pages, used = [], [], 0

    def append(source_id, path, raw, *, binding=None, sha=None, origin='authoritative_file'):
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

        facts = build_context(root, reference=coding, exclude=project.get('protected', ()),
                              allowed_roots=allowed_roots, denied_paths=denied_paths)
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
    if input_access:
        manifest['input_access'] = 'conversation_readonly'
    prompt = '\n\n'.join(pages) if pages else ('实际项目目录：' + str(root) + '\n')
    if len(prompt.encode()) > limits.context_bytes:
        reject('Context with trusted header exceeds initial budget', 'resource_limit')
    manifest['digest'] = digest(manifest)
    return manifest, prompt
