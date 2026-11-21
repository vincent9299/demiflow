"""Static checks for code changed through the workbench or another caller."""
import ast
from hashlib import sha256
from pathlib import Path
import time

from .definition_exports import EXPORT_FIELDS, exports
from .flow_check import FlowChecker
from .platform import content_digest
from .platform_environment import read_bounded
from .project_rules import RULES_ID, check_layout, diagnostic
from .schema import ForgeError


# A turn covers a finite set of variants. The checker already limits each graph.
MAX_VARIANTS = 24
CHECK_SECONDS = 24
MAX_SOURCE_BYTES = 16 * 1024**2


def result(diagnostics, inspected, *, applicable=True):
    invalid = any(d.get('status') == 'invalid' for d in diagnostics)
    status = ('failed' if invalid else 'incomplete' if diagnostics else
              'passed' if applicable else 'not_applicable')
    def summary(value, size):
        raw = value.encode()
        return value if len(raw) <= size else raw[:size].decode('utf-8', errors='ignore') + '…'

    return dict(status=status, inspected_count=len(inspected), diagnostic_count=len(diagnostics),
                diagnostics=[dict(rule=d['rule'], path=summary(d.get('path', ''), 256), line=d.get('line'),
                                  detail=summary(d['detail'], 256)) for d in diagnostics[:4]])


def _record(sources, path, value):
    old = sources.setdefault(path, value)
    if old != value:
        raise ForgeError('Source changed between inspected flows: ' + path, code='conflict')


def check_changes(root, *, changed, variants, layout_diagnostics, coverage, catalog, catalog_id):
    """Check the same source and SDK basis as the coding turn, without execution."""
    deadline = time.monotonic() + CHECK_SECONDS
    diagnostics, inspected, sources = list(coverage), [], {}
    operators, prompts, misplaced_flows = set(), set(), set()
    layout_diagnostics = list(layout_diagnostics)
    for index, (entry, load, outside_flows) in enumerate(variants):
        available = int(deadline - time.monotonic())
        if index >= MAX_VARIANTS or available < 1:
            diagnostics.append(diagnostic('FL-10', '', '还有入口未检查，达到本轮检查预算。', status='incomplete'))
            layout_diagnostics.append(diagnostic('DF-01', '', '检查预算内未覆盖所有入口的文件角色。', status='incomplete'))
            break
        try:
            args, tables, targets, resolver, recheck = load()
            available = int(deadline - time.monotonic())
            if available < 1:
                raise ForgeError('Flow input preparation exhausted the check budget', code='resource_limit')
            report = FlowChecker(root, catalog, tables, targets,
                                 limits={'seconds': available}, resource_resolver=resolver).inspect(entry, args)
            recheck()
            for name, digest in report['binding']['sources'].items():
                _record(sources, name, digest)
            for item in report['binding']['resources'].values():
                if 'sha256' in item:
                    _record(sources, item['path'], item['sha256'])
            files = report['binding']['sources']
            entry_path = entry.partition(':')[0]
            if outside_flows:
                if not any(node['role'] in {'action', 'query'} or node['kind'] == 'materialize'
                           for node in report['nodes']):
                    continue
                layout_diagnostics.append(diagnostic('DF-02', entry_path, '新 flow 应位于 flows/。'))
                misplaced_flows.add(entry_path)
            cross_flows = {node['source'] for node in report['nodes']
                           if node['source'] != entry_path and node['source'].startswith('flows/')}
            for path in sorted(cross_flows):
                layout_diagnostics.append(diagnostic('DF-03', path, 'flow 之间通过共享算子复用逻辑。'))
            for node in report['nodes']:
                callables = list(node.get('callbacks', {}).values())
                if node.get('operator'):
                    callables.append(node['operator'])
                for operator in callables:
                    module = operator['module'].replace('.', '/')
                    path = next((value for value in (module + '.py', module + '/__init__.py') if value in files), None)
                    if path is None:
                        layout_diagnostics.append(diagnostic('DF-01', module, '无法确认算子定义位置。', status='incomplete'))
                    else:
                        operators.add((path, operator['line']))
                if node['api'].rsplit('.', 1)[-1] in {'map_prompt', 'map_prompt_async', 'agentmap_async'}:
                    prompts.update((resource['path'], None) for resource in node['source_resources'])
            inspected.append(entry)
            diagnostics.extend(report['diagnostics'])
        except (OSError, ValueError, KeyError, RecursionError) as exc:
            path = entry.partition(':')[0]
            diagnostics.append(diagnostic('FL-10', path, str(exc), status='incomplete'))
            layout_diagnostics.append(diagnostic('DF-01', path, '此入口的引用文件角色未核实。', status='incomplete'))

    if any(Path(p).parts[0] != 'data_tools' for p in changed) and not inspected and not diagnostics:
        diagnostics.append(diagnostic('FL-10', '', '没有可核对的 flow 入口。', status='incomplete'))
    paths, violations = check_layout(changed=changed, operators=sorted(operators),
                                     prompts=sorted(prompts))
    layout_diagnostics.extend(violations)
    paths.update(misplaced_flows)
    export_budget = MAX_SOURCE_BYTES
    for path in sorted(paths.intersection(changed)):
        file = Path(path)
        field = EXPORT_FIELDS.get(file.parts[0])
        if not field or file.suffix != '.py':
            continue
        if '..' in file.parts:
            layout_diagnostics.append(diagnostic('DF-02', path, '源码路径必须位于本项目目录内。'))
            continue
        try:
            raw = read_bounded(Path(root) / file, min(export_budget, 512 * 1024))
            export_budget -= len(raw)
            _record(sources, path, sha256(raw).hexdigest())
            exports(ast.parse(raw), field)
        except FileNotFoundError:
            continue
        except (ValueError, SyntaxError, RecursionError, OSError) as exc:
            layout_diagnostics.append(diagnostic('DF-02', path, str(exc)))
    remaining = MAX_SOURCE_BYTES
    for path, digest in sources.items():
        try:
            if time.monotonic() > deadline:
                raise ForgeError('Source verification exceeded the check budget', code='resource_limit')
            raw = read_bounded(Path(root) / path, remaining)
            remaining -= len(raw)
            if sha256(raw).hexdigest() != digest:
                raise ForgeError('Source changed during the checks', code='conflict')
        except (OSError, ValueError) as exc:
            diagnostics.append(diagnostic('FL-10', path, str(exc), status='incomplete'))
            layout_diagnostics.append(diagnostic('DF-01', path, str(exc), status='incomplete'))
            break
    return dict(catalog_id=catalog_id, rules_id=RULES_ID,
                source_digest=content_digest(sources), source_count=len(sources),
                native_flow=result(diagnostics, inspected[:MAX_VARIANTS], applicable=bool(inspected)),
                project_layout=result(layout_diagnostics, sorted(paths)[:MAX_VARIANTS], applicable=bool(paths)))
