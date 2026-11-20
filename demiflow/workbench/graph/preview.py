"""Bounded, disposable construction of the first native Dataset action.

Original project code is imported in place. No discovery command, launcher,
model, Dataset worker or sink is started. This does not grant run permission.
"""
from pathlib import Path
import hashlib
import importlib
import importlib.abc
import importlib.machinery
import inspect
import io
import json
import os
import sys

from demiflow.forge.schema import ForgeError
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.workbench.projects import workspace as ws


def preview(root, *, entry, params=None):
    from demiflow.workbench.worker import operation
    return operation('plan_preview', root, entry=entry, params=params)


def _import_root(root, project):
    # Discovery already verified the public declaration and exact owner. Use
    # that repository's namespace; never copy a pipeline into a generated tree.
    for declaration in project['declarations']:
        path = Path(declaration['path'])
        if path.name == 'project.json' and path.parent.name == '.demiforge':
            base = path.parent.parent
            if root.is_relative_to(base):
                return base
    return root


def _parameters(root, control, entry, path, *, table_resolver):
    if path is None:
        return {}, None
    target = ws.validate_parameter_path(root, path)
    raw = read_bounded(target, 65536)
    from demiflow.workbench.projects.discovery import _json
    document = _json(raw)
    if type(document) is not dict or type(document.get('arguments')) is not dict:
        raise ForgeError('解析配置需要 JSON arguments 对象；不会执行配置中的命令。', code='invalid_request')
    if document.get('format') in {'demiflow_forge_flow_parameters_v1', 'demiflow_forge_flow_parameters_v2'}:
        # Preserve the same declared resource binding as native run preparation.
        arguments, _, _, _ = ws.resolve_arguments(root, control, document, table_resolver=table_resolver)
    elif set(document) <= {'arguments', 'run_directory'}:
        arguments = document['arguments']
    else:
        raise ForgeError('Unsupported plan parameter document', code='invalid_request')
    count = 0
    def visit(value, depth=0):
        nonlocal count
        count += 1
        if count > 4096 or depth > 16:
            raise ForgeError('Plan parameters exceed structural budget', code='resource_limit')
        if type(value) is dict:
            for child in value.values():
                visit(child, depth + 1)
        elif type(value) is list:
            for child in value:
                visit(child, depth + 1)
    visit(arguments)
    return arguments, hashlib.sha256(raw).hexdigest()


class _Sources(importlib.abc.MetaPathFinder):
    """Read original Python bytes, bypassing stale bytecode and pinning imports."""
    def __init__(self, root):
        self.root, self.digests, self.total = root, {}, 0

    def read(self, path):
        path = Path(path)
        ws.no_symlinks(path)
        raw = read_bounded(path, 4 * 1024**2)
        name = path.relative_to(self.root).as_posix()
        digest = hashlib.sha256(raw).hexdigest()
        if name in self.digests and self.digests[name] != digest:
            raise ForgeError('Source changed during plan construction', code='conflict')
        if name not in self.digests:
            self.total += len(raw)
            if len(self.digests) >= 256 or self.total > 32 * 1024**2:
                raise ForgeError('Plan source closure exceeds budget', code='resource_limit')
            self.digests[name] = digest
        return raw

    def find_spec(self, fullname, path=None, target=None):
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec and spec.origin and Path(spec.origin).is_relative_to(self.root):
            if not spec.origin.endswith('.py'):
                raise ForgeError('Project imports must use original Python source', code='operation_rejected')
            sources = self

            class Loader(importlib.machinery.SourceFileLoader):
                def get_code(self, name):
                    return compile(sources.read(self.path), self.path, 'exec', dont_inherit=True)

            spec.loader = Loader(fullname, spec.origin)
            return spec
        return None

    def verify(self):
        for name in tuple(self.digests):
            self.read(self.root / name)


class _Discard(io.TextIOBase):
    def write(self, value):
        return len(value)


def _preview(root, *, table_resolver, entry, params=None):
    """Called only by the fixed one-shot workspace worker, never the server."""
    from contextlib import redirect_stdout
    from demiflow.execution.host.inspection import capture_pipeline, build_logical_plan_view
    from demiflow.workbench.projects.discovery import discover
    from demiflow.workbench.graph.preview_guard import enter_readonly

    root, control, _ = ws.load_workspace(root)
    project = discover(root, control)
    if not any(item['entry'] == entry and item['status'] == 'recognized' for item in project['entries']):
        raise ForgeError('Entry is not declared by this project', code='not_found')
    target, symbol = ws.validate_entry(root, entry)
    source_root = _import_root(root, project)
    sources = _Sources(source_root)
    entry_sha = hashlib.sha256(sources.read(target)).hexdigest()
    # Resolve declared, fixed table metadata through the trusted platform
    # before importing project code. Lance initializes its native runtime here
    # (including local signal sockets), which cannot happen under the guard.
    # No row scan or output creation occurs in this resolver. Project imports
    # and all pipeline construction still run under the unchanged guard.
    arguments, params_sha = _parameters(root, control, entry, params, table_resolver=table_resolver)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(source_root))
    # A registered owner may use project-local imports as well as repository
    # namespaces. Both remain inside the same bounded source snapshot.
    if root != source_root:
        sys.path.insert(0, str(root))
    sys.meta_path.insert(0, sources)
    os.chdir(root)
    enter_readonly()  # fail closed, before ANY project import/construction
    try:
        with redirect_stdout(_Discard()):
            module = target.relative_to(source_root).as_posix()[:-3].replace('/', '.')
            function = getattr(importlib.import_module(module), symbol)
            if not inspect.isfunction(function) or Path(inspect.getfile(function)) != target:
                raise ForgeError('Entry resolved outside its declared source', code='conflict')
            def construct():
                signature = inspect.signature(function)
                try:
                    signature.bind(**arguments)
                except TypeError as exc:
                    missing = [name for name, p in signature.parameters.items()
                               if p.default is inspect.Parameter.empty and name not in arguments
                               and p.kind not in {p.VAR_POSITIONAL, p.VAR_KEYWORD}]
                    detail = ('请选择配置，入口需要参数：' if params is None else '所选配置缺少入口参数：') + '、'.join(missing[:16])
                    if not missing:
                        detail = '所选配置的 arguments 与入口参数不匹配：' + str(exc)[:256]
                    raise ForgeError(detail, code='invalid_request') from exc
                return function(**arguments)
            from demiflow.workbench.graph.plan_callsite import PlanCallsites, pipeline_material
            with PlanCallsites(source_root, sources.digests) as callsites:
                result = capture_pipeline(construct, {}, on_compiled=callsites.bind_compilation)
            from demiflow.workbench.graph.plan_details import build_plan_details
            details = build_plan_details(result.compiled, sources=sources.digests, root=source_root, callsites=callsites, bounds=result.bounds)
            pipeline = pipeline_material(source_root, target, sources.digests)
        sources.verify()
        current_params_sha = (hashlib.sha256(read_bounded(ws.validate_parameter_path(root, params), 65536)).hexdigest()
                              if params is not None else None)
        if current_params_sha != params_sha:
            raise ForgeError('Parameters changed during plan construction', code='conflict')
    except PermissionError as exc:
        raise ForgeError('解析阶段需要写文件、联网或启动进程，已停止。请把这些操作放到 Dataset 执行阶段。',
                         code='operation_rejected') from exc
    basis = dict(entry=entry, params=params, entry_sha256=entry_sha,
                 params_sha256=params_sha, sources=sources.digests, dag_id=result.compiled.graph.dag_id,
                 details_sha256=content_digest(details), pipeline_sha256=content_digest(pipeline))
    basis['digest'] = content_digest(basis)
    value = dict(format='demiflow.plan_preview.v1', available=True, basis=basis,
                 kind=result.kind, provider=result.provider, entry_completed=False,
                 scope='first_action', dag=build_logical_plan_view(result.compiled.graph, declarations=result.compiled.declarations,
                                                               observation_labels=result.compiled.observation_labels),
                 limitations=['仅解析首个 Dataset 执行动作；依赖执行结果的后续动作尚未解析。',
                              '未启动 Dataset 数据读取与执行；不包含实际资源分配、运行状态或结果。'])
    for node in value['dag']['nodes']:
        call = details.get(node['occurrence_id'], {}).get('callsite', {})
        if call.get('availability') == 'available':
            node['api_call'] = {k: call['metadata'][k] for k in ('api', 'summary')}
        node['source_views'] = {kind: dict(available=detail['availability'] == 'available', referenced=detail.get('referenced', False), reason=detail['reason'])
                                for kind, detail in details.get(node['occurrence_id'], {}).items()}
    bounded_json(value, 1024**2)
    value['pipeline_source'] = dict(available=pipeline['availability'] == 'available', path=pipeline['path'])
    value['_pipeline_detail'] = pipeline
    value['_node_details'] = details
    bounded_json(value, 5 * 1024**2)
    return value
