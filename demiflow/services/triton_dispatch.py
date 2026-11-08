"""Protect Triton's mutable CPU launch state in an owned multi-stream worker.

Autotuner.run in Triton 3.8 stores call arguments on the shared kernel object.
Another Python lane can overwrite/clear them during tuning or a launch hook.
The per-kernel lock ends when dispatch returns: it never waits for GPU work and
does not serialize whole model forwards. Installed before accepting requests;
no package files are changed, and process exit releases the patch and locks.
"""
import functools
import threading

_installation = threading.Lock()
_creation = threading.Lock()


def guard_autotuner(autotuner):
    """Idempotently guard the known shared-nargs implementation, if present."""
    with _installation:
        original = autotuner.run
        if getattr(original, '_demiflow_dispatch_guard', False): return True
        code = getattr(original, '__code__', None)
        if code is None or 'nargs' not in code.co_names: return False

        @functools.wraps(original)
        def run(self, *args, **kwargs):
            lock = getattr(self, '_demiflow_dispatch_lock', None)
            if lock is None:
                with _creation:
                    lock = getattr(self, '_demiflow_dispatch_lock', None)
                    if lock is None:
                        lock = threading.RLock()
                        self._demiflow_dispatch_lock = lock
            with lock:
                return original(self, *args, **kwargs)

        run._demiflow_dispatch_guard = True
        autotuner.run = run
        return True


def install():
    # Called only by the explicitly launched model worker. CPU configuration
    # parsing and normal demiflow imports must not import a GPU dependency.
    try:
        from triton.runtime.autotuner import Autotuner
    except ImportError:
        return False
    return guard_autotuner(Autotuner)
