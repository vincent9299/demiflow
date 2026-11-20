"""Read-only source audit and disposable identity counterexamples for design C.
No run, service, business module, model, or original history is executed.
"""
import argparse
import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

BASE = '122edf4bd0c827338846abd0114f0b36cf8c1b6d'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_audit(tree):
    package = tree / 'demiflow'
    paths = sorted(package.rglob('*.py'))
    assert len(paths) <= 4096
    assert sum(p.stat().st_size for p in paths) <= 64 * 1024**2
    modules = {}
    for p in paths:
        name = '.'.join(p.relative_to(tree).with_suffix('').parts)
        if name.endswith('.__init__'):
            name = name[:-9]
        modules[name] = p
    graph = {name: set() for name in modules}
    dynamic = []
    identity_consumers = []
    def resolve(name):
        return name if name in modules else None
    def add(owner, target):
        if target in modules:
            graph[owner].add(target)
            parts = target.split('.')
            for size in range(1, len(parts)):
                if '.'.join(parts[:size]) in modules:
                    graph[owner].add('.'.join(parts[:size]))
    for name, path in modules.items():
        source = ast.parse(path.read_text(), filename=str(path))
        parent = name if path.name == '__init__.py' else name.rpartition('.')[0]
        for item in ast.walk(source):
            if isinstance(item, ast.Import):
                for alias in item.names:
                    add(name, alias.name)
            elif isinstance(item, ast.ImportFrom):
                prefix = ('.'.join(parent.split('.')[:len(parent.split('.')) - item.level + 1]) if item.level else '')
                target = '.'.join(p for p in (prefix, item.module or '') if p)
                add(name, target)
                if target in {"demiflow.execution.recovery_identity", "demiflow.forge.platform_snapshot"}:
                    identity_consumers.append(dict(consumer=name, line=item.lineno, provider=target, names=[a.name for a in item.names]))
                for alias in item.names:
                    add(name, target + '.' + alias.name)
            elif isinstance(item, ast.Call):
                func = item.func
                symbol = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ''
                if symbol in {'import_module', '_import_module', '__import__'}:
                    arg = item.args[0] if item.args else None
                    literal = arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None
                    target = literal
                    if target and target.startswith('.'):
                        try:
                            target = importlib.util.resolve_name(target, parent)
                        except (ValueError, ImportError):
                            target = None
                    if target:
                        add(name, target)
                    dynamic.append(dict(module=name, line=item.lineno, literal=literal, resolved=resolve(target)))
    def closure(roots):
        seen, pending = set(), list(roots)
        while pending:
            name = pending.pop()
            if name in seen or name not in graph:
                continue
            seen.add(name)
            pending.extend(graph[name] - seen)
        return sorted(seen)
    apps = {m for m in modules if m.startswith(('demiflow.workbench', 'demiflow.forge'))}
    dispatch = {'demiflow.cli', 'demiflow.__main__'} & modules.keys()
    core = set(modules) - apps - dispatch
    control_roots = ['demiflow.workbench.runs.manager', 'demiflow.workbench.runs.worker',
        'demiflow.workbench.runs.native_submission', 'demiflow.workbench.worker',
        'demiflow.workbench.projects.workspace', 'demiflow.forge.platform_worker']
    core_closure = closure(core)
    control_closure = closure(control_roots)
    inventory = [dict(path=str(p.relative_to(tree)), bytes=p.stat().st_size, sha256=sha(p.read_bytes())) for p in paths]
    return dict(python_files=len(paths), python_bytes=sum(x['bytes'] for x in inventory),
        inventory=inventory, conservative_core_roots=len(core), core_static_closure=len(core_closure),
        core_static_application_targets=sorted(set(core_closure) & apps),
        control_roots=control_roots, control_static_closure=len(control_closure),
        control_application_modules=sorted(set(control_closure) & apps),
        application_modules_not_in_static_control_closure=sorted(apps-set(control_closure)),
        dynamic_imports=dynamic, identity_consumers=identity_consumers,
        limitations=['All AST import statements included, including function bodies and TYPE_CHECKING.',
          'Data-valued symbol maps, arbitrary plugin names, subprocess dispatch, resources and native libraries require manual closure.',
          'Unreached application modules are NOT certified exclusions. All application Python remains included in initial proposed control identity.'])


def counterexamples(tree, scratch):
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(tree)
    from demiflow.execution import recovery_identity as identities
    from demiflow.forge.platform_snapshot import source_inventory
    from demiflow.forge.platform import limits_with
    import cloudpickle
    result = dict(import_origin=demiflow.__file__, installed_platform_identity=identities.platform_identity())
    with tempfile.TemporaryDirectory(prefix='identity-', dir=scratch) as tmp:
        root = Path(tmp) / 'demiflow'
        root.mkdir()
        for path, data in {'__init__.py':'', 'execution/engine.py':'VALUE = 1\n',
                           'workbench/ui.py':'TITLE = "old"\n', 'forge/rules.py':'RULE = 1\n',
                           'web/style.css':'body {}\n'}.items():
            p = root / path; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(data)
        original = identities.__file__
        identities.__file__ = str(root / 'execution/recovery_identity.py')
        try:
            before = identities.platform_identity()
            cases = {}
            for name, relative in [('ui_python','workbench/ui.py'),('compile_python','planning/compiler.py'),
                ('runtime_python','execution/engine.py'),('protocol_python','execution/protocol.py'),
                ('forge_python','forge/rules.py'),('css','web/style.css'),('package_document','README.md')]:
                p = root / relative; p.parent.mkdir(parents=True, exist_ok=True)
                prior = p.read_bytes() if p.exists() else None
                forge_before = source_inventory(root, limits_with())
                p.write_bytes((prior or b'') + b'\n# changed\n')
                cases[name] = dict(platform_changes=identities.platform_identity()!=before,
                    forge_file_inventory_changes=source_inventory(root, limits_with())!=forge_before)
                if prior is None: p.unlink()
                else: p.write_bytes(prior)
            pyproject = root.parent/'pyproject.toml'; pyproject.write_text('[project]\nname="demiflow"\nversion="1"\n')
            cases['external_pyproject'] = dict(platform_changes=identities.platform_identity()!=before,
                forge_note='source inventory excludes parent pyproject; platform_declaration hashes it separately')
            old = root/'execution/engine.py'; new = root/'execution/renamed.py'
            old.rename(new)
            cases['module_file_move'] = dict(platform_changes=identities.platform_identity()!=before)
            new.rename(old)
            result['mutation_cases'] = cases
        finally:
            identities.__file__ = original
        path = Path(tmp)/'identity_fixture_original.py'
        path.write_text('def function(x=1):\n    return x + 1\n\nclass Actor:\n    def __call__(self, x):\n        return x + 1\n')
        def load(name, file):
            spec=importlib.util.spec_from_file_location(name, file)
            module=importlib.util.module_from_spec(spec); sys.modules[name]=module; spec.loader.exec_module(module)
            return module
        first = load('identity_fixture_original',path)
        fn_before = identities.describe(first.function)
        class_before = identities.describe(first.Actor)
        payload = cloudpickle.dumps(first.Actor)
        path.write_text(path.read_text()+'\nUNRELATED = 2\n')
        fn_after = identities.describe(first.function)
        relocated = load('identity_fixture_moved', path)
        sys.modules.pop('identity_fixture_original'); path.unlink()
        try:
            cloudpickle.loads(payload)
        except Exception as exc:
            payload_failure = dict(type=type(exc).__name__, message=str(exc))
        else:
            raise AssertionError('Fixture must demonstrate import-by-path failure')
        result['object_cases'] = dict(unrelated_file_edit_changes_function_source_hash=fn_before['source_hash']!=fn_after['source_hash'],
            unrelated_file_edit_preserves_function_code=fn_before['code']==fn_after['code'],
            unrelated_file_edit_preserves_class_methods=class_before==identities.describe(first.Actor),
            module_move_changes_class_identity=class_before!=identities.describe(relocated.Actor),
            missing_original_module_pickle_failure=payload_failure)
        sys.modules.pop('identity_fixture_moved')
    return result


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--output',type=Path,required=True); parser.add_argument('--scratch',type=Path,required=True)
    args=parser.parse_args(); tree=Path(__file__).resolve().parents[3]
    args.scratch.mkdir(parents=True,exist_ok=True)
    assert args.scratch.resolve().is_relative_to(Path('/tmp'))
    manifest=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120/context-manifest.json')
    context=json.loads(manifest.read_text())
    verified=[]
    for item in context['files']:
        raw=Path(item['snapshot_path']).read_bytes()
        assert len(raw)==item['bytes'] and sha(raw)==item['sha256'],item['snapshot_path']
        verified.append(dict(path=item['snapshot_path'],sha256=sha(raw)))
    result=dict(format='demiflow_identity_design_probe_v1',base_main_sha=BASE,
        source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip(),
        interpreter=sys.executable,python=platform.python_version(),context_manifest_sha256=sha(manifest.read_bytes()),
        context_verified=verified,source_audit=source_audit(tree),counterexamples=counterexamples(tree,args.scratch),
        evidence_scope='static source audit and disposable fixtures; no business execution and no cross-version recovery proof')
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in {'context_verified','source_audit'}},ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result['source_audit'].items() if k not in {'inventory','dynamic_imports','control_application_modules','application_modules_not_in_static_control_closure','identity_consumers'}},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
