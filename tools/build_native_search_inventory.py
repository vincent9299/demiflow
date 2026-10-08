"""Rebuild the complete static source inventory; reset all validation claims.

Follow with audit_native_search.py and new version-matched validation evidence.
AST-derived configuration hints are candidates, not a substitute for setup/init.
No adapter code is imported and no network requests are made.
"""
import ast
import hashlib
import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / 'demiflow/_vendor/searxng'
CAPABILITIES = {'about', 'engine_type', 'categories', 'paging', 'max_page', 'safesearch',
                'language_support', 'language', 'time_range_support', 'network', 'timeout'}


def describe(path):
    tree = ast.parse(path.read_text())
    record = {'file': str(path.relative_to(VENDOR)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
              'declared': {}, 'unset_configuration': [], 'dynamic_attributes': [], 'imports': [],
              'relative_imports': [], 'type_checking_imports': [], 'functions': [], 'setup_config_keys': [],
              'migration': 'bundled_native', 'fixture_validation': 'not_run', 'live_validation': 'not_run'}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            record['functions'].append(node.name)
            if node.name in ('setup', 'init'):
                args = {arg.arg for arg in node.args.args}
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Subscript) and isinstance(sub.value, ast.Name) and sub.value.id in args:
                        if isinstance(sub.slice, ast.Constant) and isinstance(sub.slice.value, str):
                            record['setup_config_keys'].append(sub.slice.value)
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            name = target.id
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                if name in CAPABILITIES:
                    record['dynamic_attributes'].append(name)
                continue
            if name in CAPABILITIES:
                record['declared'][name] = value
            if (value is None or value == '') and not name.startswith('_'):
                record['unset_configuration'].append(name)
    type_nodes = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and ((isinstance(node.test, ast.Name) and node.test.id == 'TYPE_CHECKING') or
                                        (isinstance(node.test, ast.Attribute) and node.test.attr == 'TYPE_CHECKING')):
            type_nodes.update(id(n) for body in node.body for n in ast.walk(body))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            record['imports'].extend(n.name for n in node.names)
            if id(node) in type_nodes:
                record['type_checking_imports'].extend(n.name for n in node.names)
        elif isinstance(node, ast.ImportFrom):
            key = 'relative_imports' if node.level else 'imports'
            record[key].append('.' * node.level + (node.module or ''))
            if id(node) in type_nodes:
                record['type_checking_imports'].append('.' * node.level + (node.module or ''))
    for key in ('imports', 'relative_imports', 'type_checking_imports', 'setup_config_keys', 'unset_configuration', 'dynamic_attributes'):
        record[key] = sorted(set(record[key]))
    return record


def main():
    modules = {p.stem: describe(p) for p in sorted((VENDOR/'searx/engines').glob('*.py'))}
    settings = yaml.safe_load((VENDOR/'searx/settings.yml').read_text())
    sources = [{'name': c['name'], 'module': c['engine'], 'configuration_keys': sorted(c),
                'disabled_default': c.get('disabled', False), 'inactive': c.get('inactive', False),
                'module_registered': c['engine'] in modules, 'migration': 'bundled_native',
                'fixture_validation': 'not_run', 'live_validation': 'not_run'} for c in settings['engines']]
    external = {n.split('.')[0] for r in modules.values() for n in r['imports'] if n not in r['type_checking_imports']}
    external -= set(sys.stdlib_module_names) | {'searx'}
    requirements = [s.strip() for s in (VENDOR/'requirements.txt').read_text().splitlines()
                    if s.strip() and not s.lstrip().startswith('#')]
    result = {'scope': 'ALL engine modules and ALL configured named sources; existence is not availability',
              'source': 'local supplied SearXNG snapshot; upstream git revision not supplied',
              'modules': modules, 'sources': sources, 'external_adapter_imports': sorted(external),
              'requirements': requirements,
              'baseline_id': json.loads((VENDOR/'BASELINE.json').read_text())['baseline_id']}
    (ROOT/'docs/native-search-inventory.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(f'Rebuilt {len(modules)-1} adapters / {len(sources)} sources; prior validation claims reset')


if __name__ == '__main__':
    main()
