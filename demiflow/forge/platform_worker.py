"""Fixed inspection subprocess protocol with bounded output and lifetime."""
from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time

# Absolute script launches under -I bootstrap only this trusted package.
if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from demiflow.forge import operation_budget


def _cancel(signum, frame):
    # selectors intentionally swallows InterruptedError to retry interrupted
    # system calls. Cancellation must unwind to process-group cleanup.
    raise RuntimeError("Platform inspection cancelled")


def bind_parent():
    """Close the startup race when a worker outlives its launching parent."""
    import ctypes
    expected = int(os.environ.get('DEMIFLOW_INSPECTION_PARENT', os.getppid()))
    if os.getppid() != expected or ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != expected:
        raise RuntimeError('Inspection parent exited before worker lifetime was bound')
    operation_budget.bind_worker()


def inspection_environment(pass_fds=(), *, allow_preparation=True):
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1", "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1",
        "CUDA_VISIBLE_DEVICES": "",
        "TOKIO_WORKER_THREADS": "2", "RAYON_NUM_THREADS": "2",
        "LANCE_CPU_THREADS": "2",
        # glibc otherwise reserves a large arena for each native thread. Those
        # reservations consume RLIMIT_AS even when a projected Case is tiny.
        # Bound allocator arenas before process startup; keep worker limits.
        "MALLOC_ARENA_MAX": "2",
        "DEMIFLOW_INSPECTION_PARENT": str(os.getpid()),
    }
    from demiflow.resources.process_observation import inherited_unit, UNIT_ENV
    if os.environ.get("DEMIFLOW_FORGE_DOMAIN"):
        env["DEMIFLOW_FORGE_DOMAIN"]=os.environ["DEMIFLOW_FORGE_DOMAIN"]
    if os.environ.get("TMPDIR"):
        env["TMPDIR"]=os.environ["TMPDIR"]
    unit_fds=inherited_unit()
    if unit_fds:env[UNIT_ENV]=str(unit_fds[0])
    preparation_fds=pass_fds if allow_preparation else ()
    if allow_preparation and not preparation_fds and os.environ.get('DEMIFLOW_FORGE_PREPARATION_FD'):
        preparation_fds=(int(os.environ['DEMIFLOW_FORGE_PREPARATION_FD']),)
        os.fstat(preparation_fds[0])
    if preparation_fds:
        env['DEMIFLOW_FORGE_PREPARATION_FD']=str(preparation_fds[0])
    return env, tuple(dict.fromkeys((*unit_fds,*preparation_fds)))


@operation_budget.controlled
def capture(argv, request, limits, *, response_formats=None, input_limit=16384, pass_fds=(), tree_rss_limit=None):
    from .platform import bounded_json
    from .schema import ForgeError
    # One-shot calls inherit the remaining control scope; reusable readers
    # use inspection_environment directly and never bind this deadline.
    deadline = operation_budget.child_deadline(limits['timeout_seconds'])

    env, inherited_fds = inspection_environment(pass_fds)
    env.update(operation_budget.worker_environment(deadline))
    with tempfile.TemporaryDirectory(prefix="demiflow-platform-") as scratch:
        with tempfile.TemporaryFile() as input_file:
            input_file.write(bounded_json(request, input_limit))
            input_file.seek(0)
            operation_budget.check(deadline)
            process = subprocess.Popen(argv, stdin=input_file, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=scratch, env=env, start_new_session=True, pass_fds=inherited_fds)
            outputs = {"stdout": bytearray(), "stderr": bytearray()}
            ceilings = {"stdout": limits["catalog_bytes"], "stderr": limits["stderr_bytes"]}
            selector = selectors.DefaultSelector()
            handles_term = threading.current_thread() is threading.main_thread()
            previous_term = signal.getsignal(signal.SIGTERM) if handles_term else None
            try:
                if handles_term:
                    signal.signal(signal.SIGTERM, _cancel)
                for name in outputs:
                    pipe = getattr(process, name)
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, name)
                while selector.get_map():
                    if tree_rss_limit is not None:
                        from demiflow.resources.process_observation import descendants
                        if sum(p['rss_bytes'] for p in descendants(process.pid,include_owner=True).values())>tree_rss_limit:
                            raise ForgeError('Managed inspection tree exceeds RSS reservation',code='resource_limit')
                    expired = time.monotonic() >= deadline
                    if expired and process.poll() is None:
                        raise ForgeError("Platform inspection timed out", code='resource_limit')
                    # If already stopped, drain its bounded terminal response.
                    # A ready fact is not rewritten as failure for a late read.
                    available = selector.select(timeout=min(.1, max(0, deadline-time.monotonic())))
                    if expired and not available:
                        # A descendant may still own a pipe after its parent
                        # exits. Do not wait indefinitely for an unknown reply.
                        raise ForgeError("Platform inspection response incomplete at deadline", code='resource_limit')
                    for key, _ in available:
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        target = outputs[key.data]
                        if len(target) + len(chunk) > ceilings[key.data]:
                            raise ForgeError(f"Platform inspection {key.data} exceeds byte budget", code='resource_limit')
                        target.extend(chunk)
                try:
                    process.wait(timeout=max(.001, deadline-time.monotonic()))
                except subprocess.TimeoutExpired as exc:
                    raise ForgeError("Platform inspection timed out", code='resource_limit') from exc
                if process.returncode:
                    # Imported modules are trusted; do not return arbitrary
                    # module stdout/stderr as a successful protocol response.
                    tail = ' '.join(outputs["stderr"].decode("utf-8", "replace").split())[-240:]
                    detail = f": {tail}" if tail else ""
                    raise ForgeError(f"Platform inspection process failed ({process.returncode}){detail}")
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                selector.close()
                process.stdout.close()
                process.stderr.close()
                if handles_term:
                    signal.signal(signal.SIGTERM, previous_term)
    try:
        result = json.loads(outputs["stdout"])
    except (ValueError, UnicodeError) as exc:
        raise ForgeError("Invalid platform inspection response") from exc
    allowed = response_formats or {"demiflow_forge_platform_catalog_v1", "demiflow_forge_platform_catalog_v2", "demiflow_forge_platform_catalog_v3"}
    if not isinstance(result, dict) or result.get("format") not in allowed:
        raise ForgeError("Invalid platform inspection response")
    return result


def run_inspection(modules, limits, *, version=3):
    if version not in (1, 2, 3):
        raise ValueError("Unsupported discovery version")
    script = Path(__file__).resolve()
    return capture([sys.executable, "-I", "-B", str(script)],
                    {"modules": list(modules), "limits": limits, "version": version}, limits)


def describe_fixed_table(uri, version):
    """Load native table metadata in a one-shot inspection process."""
    limits = {'timeout_seconds': 24, 'catalog_bytes': 2*1024**2, 'stderr_bytes': 64*1024}
    response = capture([sys.executable, '-I', '-B', str(Path(__file__).resolve())],
                       {'fixed_table': str(uri), 'version': version}, limits,
                       response_formats={'demiflow_forge_fixed_table_metadata_v1'})
    return response['metadata']


def main():
    package = Path(__file__).resolve().parents[1]
    bind_parent()
    from demiflow.forge.platform import bounded_json, inspect_modules, limits_with, module_scope

    raw = sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        raise ValueError("Inspection request exceeds byte budget")
    request = json.loads(raw)
    if set(request) == {'fixed_table', 'version'}:
        import resource
        from demiflow.lance.preview import describe_table
        if request['version'] is not None and (type(request['version']) is not int or request['version'] < 1):
            raise ValueError('Metadata inspection requires a positive integer or omitted table version')
        maximum = 4 * 1024**3
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        ceiling = min(n for n in (soft, hard, maximum) if n != resource.RLIM_INFINITY)
        resource.setrlimit(resource.RLIMIT_AS, (ceiling, hard))
        metadata = describe_table(request['fixed_table'], request['version'])
        sys.stdout.buffer.write(bounded_json(
            {'format': 'demiflow_forge_fixed_table_metadata_v1', 'metadata': metadata}, 2*1024**2))
        return
    if set(request) not in ({"modules", "limits"}, {"modules", "limits", "version"}):
        raise ValueError("Invalid inspection request")
    limits = limits_with(request["limits"])
    version = request.get("version", 1)
    if type(version) is not int or version not in (1, 2, 3):
        raise ValueError("Unsupported discovery version")
    result = inspect_modules(module_scope(request["modules"]), package, limits, version=version)
    sys.stdout.buffer.write(bounded_json(result, limits["catalog_bytes"]))


if __name__ == "__main__":
    main()
