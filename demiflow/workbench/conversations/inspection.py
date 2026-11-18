"""Disposable, externally confined construction of a managed candidate's plan.

This entry is selected only by the trusted workspace operation dispatcher.
It exposes neither the model channels nor controller storage to project code.
"""
from __future__ import annotations

import os
from pathlib import Path
import select
import sys
import time


def limit_native_allocations():
    """Bound native decoding in this disposable preview without reading proc."""
    import resource
    ceiling = 4 * 1024**3
    for inherited in resource.getrlimit(resource.RLIMIT_AS):
        if inherited != resource.RLIM_INFINITY:
            ceiling = min(ceiling, inherited)
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))


def preview_launcher(root):
    from .admission import lookup
    binding = lookup(root)
    if binding is None:
        return None
    deployment, project = binding
    from demiflow.forge.development.project import Project
    from demiflow.forge.schema import ForgeError
    from .development import runtime_paths
    from .sandbox import SandboxSpec, launch_sandbox
    from demiflow.resources.process_observation import UNIT_ENV
    declared = Project(project['root'])
    read = tuple(str(path) for path in [declared.root, declared.tool_root,
        *[(declared.root / name).resolve(strict=True) for name in declared.config['readonly']]])
    denied = [str(deployment.state), str(deployment.value['runtime_domain'])]
    denied.extend(config['native_credentials'] for config in deployment.providers.values() if config.get('native_credentials'))
    dependencies = runtime_paths(dict(executable=sys.executable))

    def launch(argv, *, cwd, env, input_file, deadline):
        del argv  # Only this one fixed entry can enter the qualified scope.
        environment = {key: value for key, value in env.items() if key not in {
            UNIT_ENV, 'DEMIFLOW_INSPECTION_PARENT', 'DEMIFLOW_FORGE_PREPARATION_FD', 'DEMIFLOW_FORGE_DOMAIN'}}
        environment['HOME'] = str(cwd)
        spec = SandboxSpec(read_paths=read, write_paths=(), runtime_paths=dependencies,
            protected_paths=read, denied_paths=tuple(denied), private_home=str(cwd), role='readonly',
            seconds=max(.001, deadline-time.monotonic()), rss_bytes=deployment.limits.rss_bytes,
            processes=deployment.limits.processes, file_bytes=min(64*1024**2, deployment.limits.provider_bytes),
            private_bytes=min(deployment.limits.provider_bytes, deployment.limits.private_total_bytes))
        process = launch_sandbox([sys.executable, '-I', '-B', str(Path(__file__).resolve())],
            spec=spec, env=environment, cwd=str(declared.root))
        try:
            # Input is already bounded by capture. Transfer bytes, never its FD
            # or any preparation/lease authority, into the confined process.
            raw = input_file.read(1024**2+1)
            if len(raw) > 1024**2:
                raise ForgeError('Plan request exceeds its input budget', code='resource_limit')
            os.set_blocking(process.stdin.fileno(), False)
            offset = 0
            while offset < len(raw):
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise ForgeError('Plan request exceeded its deadline', code='resource_limit')
                if not select.select([], [process.stdin], [], min(.1, remaining))[1]: continue
                try: offset += os.write(process.stdin.fileno(), raw[offset:])
                except BlockingIOError: continue
            process.stdin.close()
            return process
        except BaseException:
            process.kill(); process.wait(timeout=5)
            for stream in (process.stdin, process.stdout, process.stderr): stream.close()
            raise
    return launch


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    import json
    from demiflow.forge import operation_budget
    from demiflow.forge.platform import bounded_json
    operation_budget.bind_worker()
    raw = sys.stdin.buffer.read(1024**2+1)
    if len(raw) > 1024**2: raise ValueError('Plan request exceeds its input budget')
    request = json.loads(raw)
    if (type(request) is not dict or set(request) != {'operation', 'root', 'arguments'}
            or request['operation'] != 'plan_preview'):
        raise ValueError('Confined inspection accepts only plan_preview')
    # Lance metadata/decoding has the same finite 4-GiB ceiling as the ordinary
    # readonly worker. Lower inherited limits remain authoritative; B1 still
    # accounts the complete process tree's RSS independently.
    limit_native_allocations()
    # The kernel boundary is imposed before Python is executed. The legacy
    # guard cannot be stacked here (B1 forbids changing seccomp); its weaker
    # filesystem/network policy is unnecessary in this fixed confined entry.
    from demiflow.workbench.graph import preview_guard
    preview_guard.enter_readonly = lambda: None
    import pyarrow as pa
    pa.set_cpu_count(2); pa.set_io_thread_count(2)
    from demiflow.workbench.worker import dispatch
    result = dispatch(request)
    sys.stdout.buffer.write(bounded_json(result, 9*1024**2))


if __name__ == '__main__':
    main()
