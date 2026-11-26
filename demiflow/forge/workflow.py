"""Declarative multi-pipeline workflow (DAG) specification, static checks, and binding helpers.

A workflow is a project-owned JSON file under ``workflows/<name>.json`` with
format ``demiflow_workflow_v1``. Each node executes one registered Flow entry
(in the host project ``"self"`` or another registered project alias) with
optional parameter overrides bound to top-level workflow inputs (``$input``) or
verified upstream outputs / trained model manifests (``$from``).

This module is pure Forge logic: it never executes business code, never mutates
project files, and never imports ``demiflow.workbench``.
"""
from __future__ import annotations

from collections import deque
from hashlib import sha256
import json
from pathlib import Path
import re

from .io import no_symlinks
from .platform import content_digest
from .platform_environment import read_bounded
from .project import load_project, validate_entry, validate_parameter_path
from .project_io import relative_path
from .project_rules import diagnostic
from .schema import ForgeError

WORKFLOW_FORMAT = 'demiflow_workflow_v1'
MAX_WORKFLOW_BYTES = 256 * 1024
MAX_WORKFLOW_NODES = 64
MAX_WORKFLOW_PROJECTS = 16
MAX_WORKFLOW_EDGES = 256
MAX_WORKFLOW_INPUTS = 32
MAX_WORKFLOW_CONCURRENCY_GROUPS = 16
MAX_WORKFLOWS_PER_PROJECT = 64
MAX_OVERRIDE_DEPTH = 16
MAX_OVERRIDE_ITEMS = 256
MAX_PARALLEL = 16

_ID_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_-]{0,63}$')
_INPUT_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,63}$')
_OUTPUT_FIELDS = frozenset({'uri', 'version', 'row_count', 'schema_hash', 'dataset_id'})
_MODEL_ATTRS = frozenset({'manifest_digest', 'manifest_uri', 'final_step', 'instance_id', 'fit_key'})
_MODEL_FILE_FIELDS = frozenset({'uri', 'sha256', 'size_bytes'})
_INPUT_TYPES = frozenset({'string', 'integer', 'number', 'boolean', 'object', 'array', 'any'})
_FAILURE_POLICIES = frozenset({'fail_fast', 'continue_independent'})
_TOP_KEYS = frozenset({'format', 'name', 'description', 'projects', 'inputs', 'concurrency_groups', 'policy', 'nodes'})
_NODE_KEYS = frozenset({'id', 'title', 'description', 'project', 'entry', 'params', 'overrides', 'after', 'concurrency_group'})
_INPUT_SPEC_KEYS = frozenset({'type', 'default', 'required', 'description'})
_POLICY_KEYS = frozenset({'max_parallel', 'on_failure'})


def validate_workflow_path(root: Path | str, path: str) -> Path:
    """Ensure ``path`` is a project-relative ``workflows/.../*.json`` path without symlinks or hidden parts."""
    if type(path) is not str or not path or len(path.encode('utf-8')) > 512:
        raise ForgeError('Workflow path must be a non-empty relative path', code='invalid_request')
    rel = Path(path)
    if rel.is_absolute() or len(rel.parts) < 2 or rel.parts[0] != 'workflows':
        raise ForgeError('Workflow files must live under workflows/*.json', code='invalid_request')
    if any(part.startswith('.') or part in {'..', ''} for part in rel.parts) or '\\' in path:
        raise ForgeError('Workflow path must not contain hidden or parent segments', code='invalid_request')
    target = relative_path(Path(root).absolute(), path)
    if target.suffix != '.json':
        raise ForgeError('Workflow files must use the .json suffix', code='invalid_request')
    return target


def is_valid_project_reference(ref) -> bool:
    """Logical project reference (registered project name or workspace id). Rejects filesystem paths."""
    if type(ref) is not str or not ref or ref != ref.strip() or len(ref.encode('utf-8')) > 256:
        return False
    if any(ord(ch) < 32 for ch in ref):
        return False
    if ref.startswith(('/', '~', '.')) or '/' in ref or '\\' in ref or '://' in ref or '..' in ref:
        return False
    if len(ref) >= 2 and ref[1] == ':' and ref[0].isalpha():
        return False
    return True


def parse_from_ref(expr: str) -> dict | None:
    """Parse a ``$from`` expression referencing an upstream node's output table or trained model.

    Supported forms:
      - ``<node>.outputs.<name>`` (full fixed-version Lance table ref ``{uri, version}``)
      - ``<node>.outputs.<name>.(uri|version|row_count|schema_hash|dataset_id)``
      - ``<node>.models.<model_key>.(manifest_digest|manifest_uri|final_step|instance_id|fit_key)``
      - ``<node>.models.<model_key>.files.<file_key>.(uri|sha256|size_bytes)``
      - ``<node>.models.<model_key>.steps.<step>.files.<file_key>.(uri|sha256|size_bytes)``
    """
    if type(expr) is not str or not expr or len(expr) > 512:
        return None
    parts = expr.split('.', 2)
    if len(parts) < 3:
        return None
    node_id, namespace, rest = parts
    if not _ID_RE.fullmatch(node_id):
        return None

    if namespace == 'outputs':
        sub = rest.split('.')
        if len(sub) == 1 and _INPUT_NAME_RE.fullmatch(sub[0]):
            return {'node': node_id, 'kind': 'output', 'name': sub[0], 'field': None, 'expr': expr}
        if len(sub) == 2 and _INPUT_NAME_RE.fullmatch(sub[0]) and sub[1] in _OUTPUT_FIELDS:
            return {'node': node_id, 'kind': 'output', 'name': sub[0], 'field': sub[1], 'expr': expr}
        return None

    if namespace == 'models':
        m_head = rest.split('.', 1)
        if len(m_head) != 2:
            return None
        model_key, tail = m_head
        if not re.fullmatch(r'^[A-Za-z0-9_:-]{1,128}$', model_key):
            return None
        if tail in _MODEL_ATTRS:
            return {
                'node': node_id,
                'kind': 'model_attr',
                'model_key': model_key,
                'attr': tail,
                'expr': expr,
            }
        step = None
        if tail.startswith('steps.'):
            step_parts = tail[len('steps.'):].split('.', 1)
            if len(step_parts) != 2 or not step_parts[0].isdigit() or int(step_parts[0]) < 1:
                return None
            step = int(step_parts[0])
            tail = step_parts[1]
        if tail.startswith('files.'):
            file_tail = tail[len('files.'):]
            dot = file_tail.rfind('.')
            if dot <= 0:
                return None
            file_key, field = file_tail[:dot], file_tail[dot + 1:]
            if field not in _MODEL_FILE_FIELDS or not file_key or '..' in file_key or '\\' in file_key or file_key.startswith('/'):
                return None
            return {
                'node': node_id,
                'kind': 'model_file',
                'model_key': model_key,
                'step': step,
                'file_key': file_key,
                'field': field,
                'expr': expr,
            }
        return None

    return None


def _check_input_value_type(expected_type: str, value) -> bool:
    if expected_type == 'any':
        return True
    if expected_type == 'string':
        return type(value) is str
    if expected_type == 'integer':
        return type(value) is int
    if expected_type == 'number':
        return type(value) in (int, float)
    if expected_type == 'boolean':
        return type(value) is bool
    if expected_type == 'object':
        return type(value) is dict
    if expected_type == 'array':
        return type(value) is list
    return False


def _scan_overrides(value, *, path: str, node_id: str, diagnostics: list[dict],
                    from_refs: list[dict], input_refs: list[dict]):
    """Collect ``$from`` / ``$input`` references and enforce depth/item budgets."""
    count = 0

    def walk(item, target_param: str | None, depth: int):
        nonlocal count
        count += 1
        if count > MAX_OVERRIDE_ITEMS:
            diagnostics.append(diagnostic('WF-01', path, f'节点 {node_id} 的 overrides 元素数量超过上限 ({MAX_OVERRIDE_ITEMS})。'))
            return
        if depth > MAX_OVERRIDE_DEPTH:
            diagnostics.append(diagnostic('WF-01', path, f'节点 {node_id} 的 overrides 嵌套深度超过上限 ({MAX_OVERRIDE_DEPTH})。'))
            return
        if type(item) is dict:
            if '$from' in item or '$input' in item:
                if set(item) == {'$from'}:
                    parsed = parse_from_ref(item['$from'])
                    if parsed is None:
                        diagnostics.append(diagnostic(
                            'WF-03', path,
                            f'节点 {node_id} 参数 {target_param or ""} 的 $from 表达式无效：{item["$from"]!r}。',
                        ))
                    else:
                        from_refs.append({**parsed, 'target_param': target_param})
                    return
                if set(item) == {'$input'}:
                    name = item['$input']
                    if type(name) is not str or not _INPUT_NAME_RE.fullmatch(name):
                        diagnostics.append(diagnostic(
                            'WF-03', path,
                            f'节点 {node_id} 参数 {target_param or ""} 的 $input 名称无效：{name!r}。',
                        ))
                    else:
                        input_refs.append({'name': name, 'target_param': target_param})
                    return
                diagnostics.append(diagnostic(
                    'WF-01', path,
                    f'节点 {node_id} 的引用对象只能单独包含 "$from" 或 "$input" 键。',
                ))
                return
            for k, v in item.items():
                if type(k) is not str or not k:
                    diagnostics.append(diagnostic('WF-01', path, f'节点 {node_id} 的 overrides 包含非法键。'))
                    continue
                next_param = k if target_param is None else f'{target_param}.{k}'
                walk(v, next_param, depth + 1)
        elif type(item) is list:
            for idx, v in enumerate(item):
                next_param = f'{target_param}[{idx}]' if target_param else f'[{idx}]'
                walk(v, next_param, depth + 1)
        elif item is not None and type(item) not in (str, int, float, bool):
            diagnostics.append(diagnostic('WF-01', path, f'节点 {node_id} 的 overrides 仅允许有限 JSON 值。'))

    walk(value, None, 0)


def load_workflow(root: Path | str, path: str) -> tuple[Path, dict, str]:
    """Read and parse a workflow JSON file within ``MAX_WORKFLOW_BYTES``."""
    root_path = Path(root).absolute()
    target = validate_workflow_path(root_path, path)
    if not target.is_file():
        raise ForgeError(f'Workflow file not found: {path}', code='not_found')
    raw = read_bounded(target, MAX_WORKFLOW_BYTES)
    digest = sha256(raw).hexdigest()
    try:
        doc = json.loads(raw.decode('utf-8'))
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ForgeError(f'Workflow JSON syntax error in {path}: {exc}', code='invalid_request') from exc
    if type(doc) is not dict:
        raise ForgeError(f'Workflow document must be a JSON object: {path}', code='invalid_request')
    return target, doc, digest


def _validate_entry_syntax(entry: str) -> bool:
    if type(entry) is not str or entry.count(':') != 1:
        return False
    file_part, symbol = entry.split(':')
    if not file_part.endswith('.py') or file_part.startswith('/') or '..' in Path(file_part).parts:
        return False
    if not symbol.isidentifier() or symbol.startswith('_'):
        return False
    parts = file_part[:-3].split('/')
    return bool(parts) and all(part.isidentifier() for part in parts)


def _validate_params_syntax(params: str) -> bool:
    if type(params) is not str or not params or params.startswith('/') or '\\' in params:
        return False
    p = Path(params)
    if p.suffix != '.json' or any(part.startswith('.') or part == '..' for part in p.parts):
        return False
    return True


def local_workflow_bindings(document, entries):
    """Identify local node sources and their explicit or uniquely bound parameter file."""
    if not isinstance(document, dict) or not isinstance(document.get('nodes'), list):
        return
    for node in document['nodes'][:MAX_WORKFLOW_NODES]:
        if not isinstance(node, dict) or node.get('project', 'self') != 'self':
            continue
        entry, params = node.get('entry'), node.get('params')
        if not _validate_entry_syntax(entry):
            continue
        if params is None and len(entries.get(entry, ())) == 1:
            params = entries[entry][0]
        yield entry, params if _validate_params_syntax(params) else None


def validate_workflow(root: Path | str, path: str, *, control: dict | None = None,
                      project_resolver=None) -> dict:
    """Statically validate one ``workflows/<name>.json`` file and project its DAG structure.

    ``project_resolver``, when provided (by Workbench or CLI), is called as
    ``project_resolver(alias, logical_ref)`` and should return a dict::

        {
            "resolved": True/False,
            "workspace_id": "...",
            "name": "...",
            "root": "/abs/path",
            "control": {...},        # loaded workspace.json declaration
            "error": "...",          # optional when resolved=False
        }
    """
    root_path = Path(root).absolute()
    diagnostics: list[dict] = []

    try:
        target, doc, digest = load_workflow(root_path, path)
    except ForgeError as exc:
        code = 'WF-07' if 'workflows/*.json' in str(exc) or 'hidden' in str(exc) else 'WF-01'
        return {
            'status': 'failed',
            'path': path,
            'digest': None,
            'format': None,
            'name': Path(path).stem if isinstance(path, str) else '',
            'description': '',
            'projects': {},
            'inputs': {},
            'concurrency_groups': {},
            'policy': {'max_parallel': 1, 'on_failure': 'fail_fast'},
            'nodes': [],
            'edges': [],
            'topo_order': [],
            'cross_project': False,
            'unverified_projects': [],
            'involved_projects': ['self'],
            'diagnostics': [diagnostic(code, str(path), str(exc))],
        }

    rel_path = target.relative_to(root_path).as_posix()
    if control is None and no_symlinks(root_path / 'workspace.json').exists():
        try:
            _, control = load_project(root_path)
        except ForgeError:
            control = None

    # 1. Top-level schema & unknown keys.
    unknown_top = sorted(set(doc) - _TOP_KEYS)
    if unknown_top:
        diagnostics.append(diagnostic('WF-01', rel_path, f'编排文件包含未知顶层字段：{", ".join(unknown_top)}。'))
    if doc.get('format') != WORKFLOW_FORMAT:
        diagnostics.append(diagnostic('WF-01', rel_path, f'编排文件 format 必须为 "{WORKFLOW_FORMAT}"。'))

    name = doc.get('name')
    if type(name) is not str or not name.strip() or len(name.encode('utf-8')) > 256 or any(ord(c) < 32 for c in name):
        diagnostics.append(diagnostic('WF-01', rel_path, '编排 name 必须为 1..256 字节的非空名称。'))
        name = Path(rel_path).stem

    description = doc.get('description', '')
    if description is None:
        description = ''
    if type(description) is not str or len(description.encode('utf-8')) > 4096:
        diagnostics.append(diagnostic('WF-01', rel_path, '编排 description 必须为不超过 4 KiB 的文本。'))
        description = ''

    # 2. Projects table (logical names/ids only, strictly no paths).
    raw_projects = doc.get('projects', {})
    projects: dict[str, str] = {}
    if type(raw_projects) is not dict:
        diagnostics.append(diagnostic('WF-01', rel_path, 'projects 必须为“别名 -> 注册项目名或工作区 ID”的对象。'))
    elif len(raw_projects) > MAX_WORKFLOW_PROJECTS:
        diagnostics.append(diagnostic('WF-01', rel_path, f'引用外部项目数超过上限 ({MAX_WORKFLOW_PROJECTS})。'))
    else:
        for alias, ref in raw_projects.items():
            if type(alias) is not str or alias == 'self' or not _ID_RE.fullmatch(alias):
                diagnostics.append(diagnostic('WF-01', rel_path, f'项目别名无效或与保留名 "self" 冲突：{alias!r}。'))
                continue
            if not is_valid_project_reference(ref):
                diagnostics.append(diagnostic(
                    'WF-07', rel_path,
                    f'项目引用 {alias!r} -> {ref!r} 禁止写入宿主机路径，必须使用已注册的项目名或工作区 ID。',
                ))
                continue
            projects[alias] = ref

    # 3. Top-level inputs.
    raw_inputs = doc.get('inputs', {})
    inputs: dict[str, dict] = {}
    if type(raw_inputs) is not dict:
        diagnostics.append(diagnostic('WF-01', rel_path, 'inputs 必须为输入参数声明对象。'))
    elif len(raw_inputs) > MAX_WORKFLOW_INPUTS:
        diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 inputs 数量超过上限 ({MAX_WORKFLOW_INPUTS})。'))
    else:
        for key, spec in raw_inputs.items():
            if type(key) is not str or not _INPUT_NAME_RE.fullmatch(key):
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input 名称无效：{key!r}。'))
                continue
            if type(spec) is not dict or (set(spec) - _INPUT_SPEC_KEYS):
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input {key!r} 声明包含非法字段。'))
                continue
            in_type = spec.get('type', 'any')
            if type(in_type) is not str or in_type not in _INPUT_TYPES:
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input {key!r} 的 type 不受支持：{in_type!r}。'))
                in_type = 'any'
            has_default = 'default' in spec
            default_val = spec.get('default')
            if has_default and default_val is not None and not _check_input_value_type(in_type, default_val):
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input {key!r} 的 default 与声明类型 {in_type} 不一致。'))
            required = spec.get('required', not has_default)
            if type(required) is not bool:
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input {key!r} 的 required 必须为布尔值。'))
                required = not has_default
            in_desc = spec.get('description', '')
            if type(in_desc) is not str or len(in_desc.encode('utf-8')) > 512:
                diagnostics.append(diagnostic('WF-01', rel_path, f'顶层 input {key!r} 的 description 超长或类型错误。'))
                in_desc = ''
            norm_spec = {'type': in_type, 'required': required, 'description': in_desc}
            if has_default:
                norm_spec['default'] = default_val
            inputs[key] = norm_spec

    # 4. Concurrency groups & execution policy.
    raw_groups = doc.get('concurrency_groups', {})
    concurrency_groups: dict[str, int] = {}
    if type(raw_groups) is not dict:
        diagnostics.append(diagnostic('WF-01', rel_path, 'concurrency_groups 必须为“组名 -> 正整数并发上限”的对象。'))
    elif len(raw_groups) > MAX_WORKFLOW_CONCURRENCY_GROUPS:
        diagnostics.append(diagnostic('WF-01', rel_path, f'concurrency_groups 数量超过上限 ({MAX_WORKFLOW_CONCURRENCY_GROUPS})。'))
    else:
        for grp, cap in raw_groups.items():
            if type(grp) is not str or not _ID_RE.fullmatch(grp):
                diagnostics.append(diagnostic('WF-01', rel_path, f'并发组名无效：{grp!r}。'))
                continue
            if type(cap) is not int or not 1 <= cap <= MAX_PARALLEL:
                diagnostics.append(diagnostic('WF-01', rel_path, f'并发组 {grp!r} 上限必须为 1..{MAX_PARALLEL} 的整数。'))
                continue
            concurrency_groups[grp] = cap

    raw_policy = doc.get('policy', {})
    policy = {'max_parallel': 2, 'on_failure': 'fail_fast'}
    if type(raw_policy) is not dict:
        diagnostics.append(diagnostic('WF-01', rel_path, 'policy 必须为对象。'))
    else:
        extra_pol = sorted(set(raw_policy) - _POLICY_KEYS)
        if extra_pol:
            diagnostics.append(diagnostic('WF-01', rel_path, f'policy 包含未知字段：{", ".join(extra_pol)}。'))
        if 'max_parallel' in raw_policy:
            mp = raw_policy['max_parallel']
            if type(mp) is not int or not 1 <= mp <= MAX_PARALLEL:
                diagnostics.append(diagnostic('WF-01', rel_path, f'policy.max_parallel 必须为 1..{MAX_PARALLEL} 的整数。'))
            else:
                policy['max_parallel'] = mp
        if 'on_failure' in raw_policy:
            of = raw_policy['on_failure']
            if not isinstance(of, str) or of not in _FAILURE_POLICIES:
                diagnostics.append(diagnostic('WF-01', rel_path, 'policy.on_failure 仅支持 "fail_fast" 或 "continue_independent"。'))
            else:
                policy['on_failure'] = of

    # 5. Nodes validation.
    raw_nodes = doc.get('nodes')
    nodes: list[dict] = []
    node_by_id: dict[str, dict] = {}
    if type(raw_nodes) is not list or not raw_nodes:
        diagnostics.append(diagnostic('WF-01', rel_path, 'nodes 必须为包含至少 1 个节点的非空数组。'))
        raw_nodes = []
    elif len(raw_nodes) > MAX_WORKFLOW_NODES:
        diagnostics.append(diagnostic('WF-01', rel_path, f'编排节点数量超过上限 ({MAX_WORKFLOW_NODES})。'))
        raw_nodes = raw_nodes[:MAX_WORKFLOW_NODES]

    resolved_projects: dict[str, dict] = {}
    if project_resolver is not None:
        for alias, ref in projects.items():
            resolved_projects[alias] = project_resolver(alias, ref)

    for idx, item in enumerate(raw_nodes):
        if type(item) is not dict:
            diagnostics.append(diagnostic('WF-01', rel_path, f'nodes[{idx}] 必须为对象。'))
            continue
        extra_keys = sorted(set(item) - _NODE_KEYS)
        if extra_keys:
            diagnostics.append(diagnostic('WF-01', rel_path, f'nodes[{idx}] 包含未知字段：{", ".join(extra_keys)}。'))
        node_id = item.get('id')
        if type(node_id) is not str or not _ID_RE.fullmatch(node_id):
            diagnostics.append(diagnostic('WF-01', rel_path, f'nodes[{idx}].id 无效（须以字母开头，仅含字母/数字/_/-，最长 64 字符）。'))
            continue
        if node_id in node_by_id:
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 id 重复：{node_id}。'))
            continue

        title = item.get('title') or node_id
        if type(title) is not str or not title.strip() or len(title.encode('utf-8')) > 256:
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 title 无效。'))
            title = node_id
        node_desc = item.get('description', '')
        if node_desc is None:
            node_desc = ''
        if type(node_desc) is not str or len(node_desc.encode('utf-8')) > 1024:
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 description 超长或类型错误。'))
            node_desc = ''

        proj_alias = item.get('project', 'self')
        if not isinstance(proj_alias, str):
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 project 必须为项目别名字符串。'))
            proj_alias = ''
        elif proj_alias != 'self' and proj_alias not in projects:
            diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {node_id} 引用了未在 projects 中声明的项目别名：{proj_alias!r}。'))

        entry = item.get('entry')
        if not _validate_entry_syntax(entry):
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 entry 格式无效（应为 flows/xxx.py:func）：{entry!r}。'))
            entry = '' if type(entry) is not str else entry

        params = item.get('params')
        if params is not None and not _validate_params_syntax(params):
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 params 必须为项目内相对 .json 路径：{params!r}。'))

        # Verify registration against target project's workspace.json when available.
        if proj_alias == 'self' and control is not None and entry:
            registered_entries = control.get('entries') or {}
            if entry not in registered_entries:
                diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 引用的本机入口 {entry} 未在 workspace.json 登记。'))
            else:
                bound = registered_entries[entry]
                if params is None:
                    if len(bound) == 1:
                        params = bound[0]
                    else:
                        diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 的入口 {entry} 绑定了多个参数文件，请显式指定 params。'))
                elif params not in bound:
                    diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 的参数 {params} 未绑定到入口 {entry}。'))
        elif proj_alias != 'self' and proj_alias in resolved_projects and entry:
            ext = resolved_projects[proj_alias]
            if not ext.get('resolved'):
                diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {node_id} 的外部项目 {proj_alias}（{projects.get(proj_alias)}）未能解析：{ext.get("error", "未在项目注册表中找到")}。'))
            else:
                ext_ctrl = ext.get('control') or {}
                ext_entries = ext_ctrl.get('entries') or {}
                if entry not in ext_entries:
                    diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 引用的外部项目入口 {proj_alias}:{entry} 未在目标项目 workspace.json 登记。'))
                else:
                    bound = ext_entries[entry]
                    if params is None:
                        if len(bound) == 1:
                            params = bound[0]
                        else:
                            diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 在外部项目 {proj_alias} 的入口 {entry} 绑定了多个参数文件，请显式指定 params。'))
                    elif params not in bound:
                        diagnostics.append(diagnostic('WF-04', rel_path, f'节点 {node_id} 的参数 {params} 未绑定到外部项目入口 {proj_alias}:{entry}。'))

        raw_after = item.get('after', [])
        after: list[str] = []
        if type(raw_after) is not list or any(type(x) is not str for x in raw_after):
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 after 必须为上游节点 ID 字符串数组。'))
        else:
            after = list(dict.fromkeys(raw_after))

        grp = item.get('concurrency_group')
        if grp is not None:
            if type(grp) is not str or grp not in concurrency_groups:
                diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {node_id} 引用了未声明的并发组：{grp!r}。'))
                grp = None

        raw_overrides = item.get('overrides', {})
        from_refs: list[dict] = []
        input_refs: list[dict] = []
        if type(raw_overrides) is not dict:
            diagnostics.append(diagnostic('WF-01', rel_path, f'节点 {node_id} 的 overrides 必须为对象。'))
            raw_overrides = {}
        else:
            _scan_overrides(
                raw_overrides,
                path=rel_path,
                node_id=node_id,
                diagnostics=diagnostics,
                from_refs=from_refs,
                input_refs=input_refs,
            )

        for iref in input_refs:
            if iref['name'] not in inputs:
                diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {node_id} 引用了未声明的顶层 input：{iref["name"]}。'))

        node_obj = {
            'id': node_id,
            'title': title,
            'description': node_desc,
            'project': proj_alias,
            'project_ref': (control.get('name') if control else root_path.name) if proj_alias == 'self' else projects.get(proj_alias, proj_alias),
            'entry': entry,
            'params': params,
            'overrides': raw_overrides,
            'after': after,
            'concurrency_group': grp,
            'from_refs': from_refs,
            'input_refs': input_refs,
            'deps': [],
        }
        nodes.append(node_obj)
        node_by_id[node_id] = node_obj

    # 6. Build edges & check dependency existence + DAG acyclicity.
    edges: list[dict] = []
    edge_keys: set[tuple[str, str, str, str]] = set()
    for node in nodes:
        nid = node['id']
        deps: list[str] = []
        for dep in node['after']:
            if dep == nid:
                diagnostics.append(diagnostic('WF-02', rel_path, f'节点 {nid} 不能依赖自身。'))
                continue
            if dep not in node_by_id:
                diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {nid} 的 after 引用了不存在的节点：{dep}。'))
                continue
            if dep not in deps:
                deps.append(dep)
            ek = (dep, nid, 'after', '')
            if ek not in edge_keys:
                edge_keys.add(ek)
                edges.append({'from': dep, 'to': nid, 'kind': 'after', 'artifact': '', 'target_param': '', 'expr': ''})

        for fref in node['from_refs']:
            dep = fref['node']
            if dep == nid:
                diagnostics.append(diagnostic('WF-02', rel_path, f'节点 {nid} 的 $from 不能引用自身输出。'))
                continue
            if dep not in node_by_id:
                diagnostics.append(diagnostic('WF-03', rel_path, f'节点 {nid} 的 $from 引用了不存在的上游节点：{dep}。'))
                continue
            if dep not in deps:
                deps.append(dep)
            kind = 'data' if fref['kind'] == 'output' else 'model'
            if fref['kind'] == 'output':
                artifact = fref['name'] + (f'.{fref["field"]}' if fref.get('field') else '')
            elif fref['kind'] == 'model_attr':
                artifact = f'models.{fref["model_key"]}.{fref["attr"]}'
            else:
                step_txt = f'.step{fref["step"]}' if fref.get('step') is not None else ''
                artifact = f'models.{fref["model_key"]}{step_txt}.{fref["file_key"]}'
            ek = (dep, nid, kind, fref['target_param'] or artifact)
            if ek not in edge_keys:
                edge_keys.add(ek)
                edges.append({
                    'from': dep,
                    'to': nid,
                    'kind': kind,
                    'artifact': artifact,
                    'target_param': fref['target_param'] or '',
                    'expr': fref['expr'],
                })
        node['deps'] = deps

    if len(edges) > MAX_WORKFLOW_EDGES:
        diagnostics.append(diagnostic('WF-01', rel_path, f'编排依赖边数超过上限 ({MAX_WORKFLOW_EDGES})。'))
        edges = edges[:MAX_WORKFLOW_EDGES]

    # Kahn's topological sort for cycle detection.
    in_degree = {n['id']: len(n['deps']) for n in nodes}
    children: dict[str, list[str]] = {n['id']: [] for n in nodes}
    for n in nodes:
        for dep in n['deps']:
            children[dep].append(n['id'])
    queue = deque(n['id'] for n in nodes if in_degree[n['id']] == 0)
    topo_order: list[str] = []
    while queue:
        cur = queue.popleft()
        topo_order.append(cur)
        for nxt in children[cur]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)
    if len(topo_order) != len(nodes):
        cyclic = [nid for nid, deg in in_degree.items() if deg > 0]
        diagnostics.append(diagnostic('WF-02', rel_path, f'编排依赖图存在环：{", ".join(cyclic[:8])}。'))
        topo_order = []

    involved_projects: list[str] = []
    for n in nodes:
        if n['project'] not in involved_projects:
            involved_projects.append(n['project'])
    cross_project = any(p != 'self' for p in involved_projects)

    return {
        'status': 'failed' if diagnostics else 'passed',
        'path': rel_path,
        'digest': digest,
        'format': WORKFLOW_FORMAT,
        'name': name,
        'description': description,
        'projects': projects,
        'inputs': inputs,
        'concurrency_groups': concurrency_groups,
        'policy': policy,
        'nodes': nodes,
        'edges': edges,
        'topo_order': topo_order,
        'cross_project': cross_project,
        'unverified_projects': [p for p in involved_projects if p in projects and p not in resolved_projects],
        'involved_projects': involved_projects,
        'diagnostics': diagnostics,
    }


def discover_workflows(root: Path | str, *, control: dict | None = None,
                       project_resolver=None, paths=None, exclude=()) -> list[dict]:
    """Summarize registered declarations and visible drafts, within discovery limits."""
    from .project import discover_project_sources
    root_path = Path(root).absolute()
    if paths is None:
        view = discover_project_sources(root_path, exclude=exclude, role_names=('workflows',))
        paths = view['roles']['workflows']
        if control is None:
            control = view['control']
    registered = list((control or {}).get('workflows') or ())
    ordered_paths = [p for p in dict.fromkeys([*registered, *paths])
                     if not any(p == x or p.startswith(x + '/') for x in exclude)][:MAX_WORKFLOWS_PER_PROJECT]
    registered_set = set(registered)
    items: list[dict] = []
    for rel in ordered_paths:
        spec = validate_workflow(root_path, rel, control=control, project_resolver=project_resolver)
        items.append({
            'path': rel,
            'registered': rel in registered_set,
            'status': spec['status'],
            'name': spec['name'],
            'description': spec['description'],
            'digest': spec['digest'],
            'node_count': len(spec['nodes']),
            'edge_count': len(spec['edges']),
            'cross_project': spec['cross_project'],
            'unverified_projects': spec['unverified_projects'],
            'involved_projects': spec['involved_projects'],
            'projects': spec['projects'],
            'diagnostic_count': len(spec['diagnostics']),
            'diagnostics': spec['diagnostics'][:8],
        })
    return items


def validate_workflow_inputs(spec: dict, supplied: dict | None = None) -> dict:
    """Validate runtime workflow ``inputs`` against ``spec['inputs']`` and fill declared defaults."""
    supplied = {} if supplied is None else supplied
    if type(supplied) is not dict:
        raise ForgeError('Workflow inputs must be a JSON object', code='invalid_request')
    declared = spec.get('inputs') or {}
    unknown = sorted(set(supplied) - set(declared))
    if unknown:
        raise ForgeError(f'Unknown workflow inputs: {", ".join(unknown)}', code='invalid_request')
    resolved = {}
    for key, info in declared.items():
        if key in supplied:
            val = supplied[key]
        elif 'default' in info:
            val = info['default']
        elif info.get('required', True):
            raise ForgeError(f'Missing required workflow input: {key}', code='invalid_request')
        else:
            continue
        if val is not None and not _check_input_value_type(info.get('type', 'any'), val):
            raise ForgeError(
                f'Workflow input {key!r} expects type {info.get("type")}, got {type(val).__name__}',
                code='invalid_request',
            )
        resolved[key] = val
    return resolved


def resolve_from_expression(parsed: dict, upstream_artifacts: dict[str, dict]):
    """Resolve one parsed ``$from`` expression against verified upstream node artifacts.

    ``upstream_artifacts[node_id]`` has shape::

        {
            "outputs": {
                "<name>": {"uri": str, "version": int, "row_count": int, "schema_hash": str, "dataset_id": str}
            },
            "models": {
                "<key>": {
                    "manifest_digest": str,
                    "manifest_uri": str,
                    "final_step": int,
                    "instance_id": str,
                    "fit_key": str,
                    "files": {"<file_key>": {"uri": str, "sha256": str, "size_bytes": int}},
                    "steps": {"<step>": {"step": int, "model_format": str, "files": {...}}},
                }
            }
        }
    """
    node_id = parsed['node']
    bundle = upstream_artifacts.get(node_id)
    if bundle is None:
        raise ForgeError(f'WF-06: 上游节点 {node_id} 尚未产出已核验交付物。', code='conflict')

    if parsed['kind'] == 'output':
        outputs = bundle.get('outputs') or {}
        name = parsed['name']
        item = outputs.get(name)
        if item is None and name == 'default' and len(outputs) == 1:
            item = next(iter(outputs.values()))
        if item is None:
            avail = ', '.join(sorted(outputs)) or '无'
            raise ForgeError(
                f'WF-06: 上游节点 {node_id} 不存在已提交输出表 {name!r}（可用：{avail}）。',
                code='conflict',
            )
        field = parsed.get('field')
        if field is None:
            return {'uri': item['uri'], 'version': item['version']}
        if field not in item:
            raise ForgeError(f'WF-06: 上游输出 {node_id}.outputs.{name} 缺少字段 {field}。', code='conflict')
        return item[field]

    models = bundle.get('models') or {}
    mkey = parsed['model_key']
    model = models.get(mkey)
    if model is None and mkey == 'default' and len(models) == 1:
        model = next(iter(models.values()))
    if model is None:
        avail = ', '.join(sorted(models)) or '无'
        raise ForgeError(
            f'WF-06: 上游节点 {node_id} 不存在已提交训练模型 {mkey!r}（可用：{avail}）。',
            code='conflict',
        )

    if parsed['kind'] == 'model_attr':
        attr = parsed['attr']
        if attr not in model:
            raise ForgeError(f'WF-06: 上游模型 {node_id}.models.{mkey} 缺少属性 {attr}。', code='conflict')
        return model[attr]

    if parsed['kind'] == 'model_file':
        step = parsed.get('step')
        if step is not None:
            step_obj = (model.get('steps') or {}).get(str(step))
            if step_obj is None:
                avail_steps = ', '.join(sorted((model.get('steps') or {}).keys())) or '无'
                raise ForgeError(
                    f'WF-06: 上游模型 {node_id}.models.{mkey} 不存在 checkpoint step {step}（可用：{avail_steps}）。',
                    code='conflict',
                )
            files = step_obj.get('files') or {}
        else:
            files = model.get('files') or {}
        file_key = parsed['file_key']
        file_obj = files.get(file_key)
        if file_obj is None:
            avail_files = ', '.join(sorted(files)) or '无'
            raise ForgeError(
                f'WF-06: 上游模型 {node_id}.models.{mkey} 不存在权重文件 {file_key!r}（可用：{avail_files}）。',
                code='conflict',
            )
        field = parsed['field']
        if field not in file_obj:
            raise ForgeError(f'WF-06: 上游模型文件 {file_key!r} 缺少字段 {field}。', code='conflict')
        return file_obj[field]

    raise ForgeError(f'WF-06: 不支持的引用类型 {parsed["kind"]!r}。', code='invalid_request')


def resolve_node_overrides(node: dict, *, inputs_values: dict,
                           upstream_artifacts: dict[str, dict]) -> tuple[dict, list[dict]]:
    """Resolve all ``$input`` and ``$from`` bindings in ``node['overrides']``."""
    records: list[dict] = []

    def transform(item, target_param: str | None, depth: int):
        if depth > MAX_OVERRIDE_DEPTH:
            raise ForgeError('Workflow override depth exceeds budget', code='resource_limit')
        if type(item) is dict:
            if set(item) == {'$input'}:
                name = item['$input']
                if name not in inputs_values:
                    raise ForgeError(f'Missing resolved workflow input: {name}', code='invalid_request')
                val = inputs_values[name]
                records.append({'param': target_param, 'kind': 'input', 'expr': name, 'value': val})
                return val
            if set(item) == {'$from'}:
                parsed = parse_from_ref(item['$from'])
                if parsed is None:
                    raise ForgeError(f'Invalid $from reference: {item["$from"]!r}', code='invalid_request')
                val = resolve_from_expression(parsed, upstream_artifacts)
                records.append({'param': target_param, 'kind': parsed['kind'], 'expr': item['$from'], 'value': val})
                return val
            return {
                k: transform(v, k if target_param is None else f'{target_param}.{k}', depth + 1)
                for k, v in item.items()
            }
        if type(item) is list:
            return [
                transform(v, f'{target_param}[{i}]' if target_param else f'[{i}]', depth + 1)
                for i, v in enumerate(item)
            ]
        return item

    resolved = transform(node.get('overrides') or {}, None, 0)
    return resolved, records


def workflow_spec_digest(spec: dict) -> str:
    """Stable digest of the normalized workflow definition."""
    return content_digest({
        'format': spec.get('format'),
        'path': spec.get('path'),
        'digest': spec.get('digest'),
        'projects': spec.get('projects'),
        'inputs': spec.get('inputs'),
        'concurrency_groups': spec.get('concurrency_groups'),
        'policy': spec.get('policy'),
        'nodes': [
            {k: n[k] for k in ('id', 'project', 'entry', 'params', 'overrides', 'after', 'concurrency_group')}
            for n in spec.get('nodes') or ()
        ],
    })
