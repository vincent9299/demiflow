"""Opt-in, owned real analysis helper for D19 acceptance, with no runtime doubles.

Uses the existing interpreter and installed dependency metadata. All writes,
sockets and children belong to one new /tmp directory. This is trusted_local,
whose managed-process cleanup does not claim OCI isolation or hard quotas.
"""
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def _digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
    path.chmod(0o600)
    return _digest(path)


@contextmanager
def private_runtime(tree, workspace, evidence):
    """Start only the production controller and bind its original PID to Web."""
    from demiflow.forge.process import process_identity
    from demiflow.workbench.analysis.trusted_local import default_limits
    root = Path(tempfile.mkdtemp(prefix='demiflow-d19-kernel-', dir='/tmp'))
    root.chmod(0o700)
    state = root / 'state'
    state.mkdir(mode=0o700)
    outbox = state / 'transfer/web-outbox'
    outbox.mkdir(parents=True)
    outbox.parent.chmod(0o700)
    outbox.chmod(0o2770)
    python = Path(sys.executable).resolve()
    files = {}
    # Bind all source/resources workers can import from the candidate platform.
    for path in sorted((tree / 'demiflow').rglob('*')):
        if path.is_file() and '__pycache__' not in path.parts and path.suffix != '.pyc':
            files[str(path.relative_to(tree))] = dict(bytes=path.stat().st_size, sha256=_digest(path))
    assert len(files) <= 2048 and sum(item['bytes'] for item in files.values()) <= 128 * 1024**2
    manifest = root / 'source.json'
    code_sha = _write(manifest, dict(format='demiflow_analysis_source_release_v1', files=files))
    dependencies = {}
    for name in ('ipykernel', 'jupyter-client', 'pyzmq', 'pyarrow', 'pylance', 'datafusion'):
        distribution = importlib.metadata.distribution(name)
        metadata = next(file for file in distribution.files if str(file).endswith('.dist-info/METADATA'))
        path = Path(distribution.locate_file(metadata)).resolve()
        dependencies[str(path)] = _digest(path)
    environment = root / 'environment.json'
    environment_sha = _write(environment, dict(format='demiflow_analysis_environment_manifest_v1', files=dependencies))
    binding = root / 'binding.json'
    limits = {**default_limits(), 'cell_seconds': 60, 'kernel_seconds': 300}
    value = dict(format='demiflow_analysis_trusted_binding_v1', deployment_id='d19-private',
                 principal='d19-reader', workspace=str(workspace), state=str(state),
                 python=str(python), python_sha256=_digest(python), code_root=str(tree),
                 code_manifest=str(manifest), code_sha256=code_sha,
                 environment_manifest=str(environment), environment_sha256=environment_sha,
                 helper_socket=str(root / 'helper.sock'), auth_socket=str(root / 'auth.sock'),
                 transfer_root=str(outbox), limits=limits, media_tool=None,
                 service_capabilities=dict(analysis_execute=True, analysis_write=True, case_annotate=True))
    _write(binding, value)
    log = (root / 'controller.log').open('wb')
    command = [str(python), '-B', '-m', 'demiflow.workbench.analysis', 'controller',
               '--execution-backend', 'trusted_local', '--binding', str(binding)]
    process = subprocess.Popen(command, cwd=tree, env={**os.environ, 'PYTHONPATH': str(tree),
                               'PYTHONDONTWRITEBYTECODE': '1'}, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 30
        while not (root / 'helper.sock').exists():
            assert process.poll() is None, (root / 'controller.log').read_text()
            assert time.monotonic() < deadline, 'owned controller did not become ready'
            time.sleep(.1)
        web_config = root / 'web.json'
        # The production loader validates this binding again in the helper.
        # Build Web metadata from that same fixed origin and the actual process.
        from demiflow.workbench.analysis import contracts as c
        _write(web_config, dict(format='demiflow_analysis_trusted_web_config_v1',
               deployment_id=value['deployment_id'], principal=value['principal'],
               helper_uid=os.geteuid(), helper_identity=process_identity(process.pid),
               helper_socket=value['helper_socket'], auth_socket=value['auth_socket'],
               transfer_root=value['transfer_root'], code_digest=code_sha,
               python_digest=value['python_sha256'], environment_digest=environment_sha,
               policy_digest=c.digest(limits), service_capabilities=value['service_capabilities']))
        evidence.mkdir(parents=True, exist_ok=True)
        _write(evidence / 'real-runtime.json', dict(root=str(root), command=command,
               source_manifest=str(manifest), source_sha256=code_sha,
               environment_manifest=str(environment), environment_sha256=environment_sha,
               mode='trusted_local', helper=process_identity(process.pid)))
        yield str(web_config)
    finally:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise AssertionError('owned analysis controller failed graceful cleanup')
        finally:
            log.close()
        assert process.returncode == 0, (root / 'controller.log').read_text()
        # Keep only this owned domain as test evidence; no shared state is touched.
        leases = [json.loads(path.read_text()) for path in (state / 'runtime').glob('j*/lease.json')]
        assert all(item['state'] == 'cleanup_complete' for item in leases), leases
