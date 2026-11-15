"""The acceptance child owns its private Ray cluster and process-global FDs."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

WORKTREE = Path(__file__).resolve().parents[1]


def _fds():
    result = {}
    for name in os.listdir('/proc/self/fd'):
        try:
            result[name] = os.readlink('/proc/self/fd/' + name)
        except FileNotFoundError:
            pass
    return result


def run_ray_api_matrix(fixture, fault):
    request = fixture.evidence_dir / 'ray-api-request.json'
    request.write_text(json.dumps({'root': str(fixture.root), 'fault': fault}))
    before = _fds()
    with (fixture.evidence_dir / 'ray-api-child.stdout').open('wb') as stdout, (fixture.evidence_dir / 'ray-api-child.stderr').open('wb') as stderr:
        process = subprocess.Popen([sys.executable, '-B', '-m', 'runtime_v3_ray_matrix_process', str(request)],
            cwd=WORKTREE, env=dict(os.environ), stdout=stdout, stderr=stderr)
        fixture._children.append(process)
        try:
            process.wait(timeout=1800)
        except BaseException:
            process.kill()
            process.wait(timeout=30)
            raise
    after = _fds()
    detail = {'pid': process.pid, 'returncode': process.returncode, 'parent_fds_before': before,
              'parent_fds_after': after, 'normal_exit': process.returncode == 0}
    (fixture.evidence_dir / 'ray-api-process.json').write_text(json.dumps(detail, indent=2))
    assert process.returncode == 0, 'Ray matrix child failed; inspect its owned stderr and case evidence'
    path = fixture.evidence_dir / 'api-matrix.json'
    assert path.stat().st_size <= 8 * 1024**2, 'matrix report exceeds its evidence budget'
    report = json.loads(path.read_text())
    report['process_boundary'] = detail
    return report


def _main(request):
    from runtime_v3_support import assert_tree_binding
    from runtime_v3_api_scenarios import api_matrix
    import ray
    assert_tree_binding()
    value = json.loads(Path(request).read_text())
    root = Path(value['root']).resolve()
    assert root.parent == Path('/tmp') and root.name.startswith('demiflow-')
    assert Path(request).resolve() == root / 'evidence/ray-api-request.json'
    inputs = {str(p.relative_to(WORKTREE)): hashlib.sha256(p.read_bytes()).hexdigest()
              for folder in ('demiflow', 'tests') for p in sorted((WORKTREE / folder).rglob('*.py'))}
    meta = {'pid': os.getpid(), 'python': sys.executable, 'ray_version': ray.__version__,
            'inputs': inputs, 'fds_before': _fds(), 'cases': []}
    def observe(case):
        meta['cases'].append({'name': case['name'], 'status': case['status'], 'fds': _fds()})
    try:
        api_matrix(SimpleNamespace(root=root, evidence_dir=root/'evidence', provider='ray_core'),
                   value['fault'], case_observer=observe)
    finally:
        meta['ray_initialized_after'] = ray.is_initialized()
        meta['fds_after_shutdown'] = _fds()
        meta['changed_inputs'] = [p for p, digest in inputs.items()
            if not (WORKTREE / p).exists() or hashlib.sha256((WORKTREE / p).read_bytes()).hexdigest() != digest]
        (root / 'evidence/ray-api-child.json').write_text(json.dumps(meta, indent=2))
    assert not meta['changed_inputs'] and not meta['ray_initialized_after']


if __name__ == '__main__':
    _main(sys.argv[1])
