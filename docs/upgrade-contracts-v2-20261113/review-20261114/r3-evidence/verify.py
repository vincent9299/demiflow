"""Reproduce only third-review changed-tool checks, not business acceptance."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[4]
out = root / 'docs/upgrade-contracts-v2-20261113'
dest = Path('/tmp/demiflow-review-r3-20261114')
dest.mkdir(exist_ok=True)
assert Path.cwd() == root
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

paths = sorted(p for p in out.rglob('*') if p.is_file()
               and not any(part.endswith('-evidence') for part in p.parts)
               and '__pycache__' not in p.parts and p.name != 'design-validation.json')
paths += [Path(__file__)]
paths += [out / 'review-20261114/r3-evidence' / name
          for name in ['source-audit.json', 'wiring-before.json']]
paths += [root / p for p in json.loads((out / 'baseline.json').read_text())['files']]
audit = json.loads((out / 'review-20261114/r3-evidence/source-audit.json').read_text())
external = audit['diagram_inputs']
assert all(digest(Path(p)) == value for p, value in external.items())
sys.path.insert(0, str(out / 'tools'))
from task_graph_rules import wiring_errors
old_plan_bytes = subprocess.check_output(
    ['git', 'show', '0677dc2:docs/upgrade-contracts-v2-20261113/plan.json'])
old_errors = wiring_errors(json.loads(old_plan_bytes)['tasks'])
assert old_errors == json.loads((out / 'review-20261114/r3-evidence/wiring-before.json').read_text())
env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=str(root))
manifest = dict(schema='demiflow.design_checks.v1', cwd=str(root), python=sys.executable,
                python_version=sys.version,
                head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                git_status=subprocess.check_output(['git', 'status', '--porcelain'], text=True),
                inputs={str(p.relative_to(root)): digest(p) for p in paths},
                external_read_only_inputs=external,
                old_plan_commit='0677dc265106628aaec627b09df9d252d3d50be6',
                old_plan_sha256=hashlib.sha256(old_plan_bytes).hexdigest(),
                reproduced_old_plan_wiring_errors=old_errors,
                commands=[], runtime_business_tests_run=False,
                not_run=['business acceptance', 'Ray', 'browser', 'performance',
                         'previous unchanged 30 evidence/scope/aggregation tool tests'])
for script in ['test_task_graph_rules.py', 'check_design.py']:
    argv = [sys.executable, str(out / 'tools' / script)]
    result = subprocess.run(argv, cwd=root, env=env, capture_output=True, text=True, timeout=60)
    log = dest / (script.removesuffix('.py') + '.log')
    log.write_text(result.stdout + result.stderr)
    manifest['commands'].append(dict(argv=argv, cwd=str(root),
        environment={key: env[key] for key in ['PYTHONDONTWRITEBYTECODE', 'PYTHONPATH']},
        exit_code=result.returncode, log=log.name, log_sha256=digest(log)))
    print(script, result.returncode, (result.stdout + result.stderr).strip()[-1000:])
manifest['inputs_unchanged_after_checks'] = all(
    digest(root / p) == value for p, value in manifest['inputs'].items())
manifest['external_inputs_unchanged_after_checks'] = all(
    digest(Path(p)) == value for p, value in external.items())
manifest['runtime_source_unchanged'] = not subprocess.check_output(
    ['git', 'diff', 'bbd93ff', '--', 'demiflow', 'tests']).strip()
(dest / 'verification.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
assert manifest['inputs_unchanged_after_checks'] and manifest['external_inputs_unchanged_after_checks']
assert manifest['runtime_source_unchanged'] and all(c['exit_code'] == 0 for c in manifest['commands'])
