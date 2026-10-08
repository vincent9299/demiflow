"""Explicit process boundary for memory-heavy terminal stages.

Pass fixed table paths/versions to a module-level function; return a small receipt.
The child exit releases its Python/Arrow/native allocations. It does not constrain
RSS or make external side effects safe to retry after an uncertain failure.
"""
from pathlib import Path
import os
import subprocess
import sys
import tempfile


def release_unused_memory():
    """Best-effort allocator release; live objects and OS RSS are not bounded."""
    import gc
    gc.collect()
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except ImportError:
        pass
    if sys.platform.startswith('linux'):
        import ctypes
        trim = getattr(ctypes.CDLL(None), 'malloc_trim', None)
        if trim is not None:
            trim(0)


def run_isolated(function, *args, timeout_s=None, **kwargs):
    import cloudpickle
    if timeout_s is not None and timeout_s <= 0:
        raise ValueError('timeout_s must be positive')
    with tempfile.TemporaryDirectory(prefix='demiflow-stage-') as directory:
        request, result = Path(directory) / 'input.pickle', Path(directory) / 'result.pickle'
        request.write_bytes(cloudpickle.dumps((function, args, kwargs)))
        process = subprocess.Popen([sys.executable, '-m', __name__, str(request), str(result)],
                                   start_new_session=True, env=os.environ.copy())
        try:
            code = process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired as exc:
            from .processes import stop_owned
            stop_owned(process)
            raise TimeoutError(f'Isolated stage exceeded {timeout_s} seconds') from exc
        except BaseException:
            from .processes import stop_owned
            stop_owned(process)
            raise
        if code != 0 or not result.exists():
            raise RuntimeError(f'Isolated stage exited {code}; inspect its durable outputs before retrying')
        ok, value = cloudpickle.loads(result.read_bytes())
        if not ok:
            raise value
        return value


def _main():
    import cloudpickle
    request, result = map(Path, sys.argv[1:])
    function, args, kwargs = cloudpickle.loads(request.read_bytes())
    try:
        value = (True, function(*args, **kwargs))
    except Exception as error:
        value = (False, error)
    result.write_bytes(cloudpickle.dumps(value))


if __name__ == '__main__':
    _main()
