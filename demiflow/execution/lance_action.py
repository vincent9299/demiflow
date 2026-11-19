"""Private registration binding consumed by the Dataset action runtime."""
from contextlib import contextmanager
from contextvars import ContextVar

_BINDING = ContextVar('demiflow_internal_lance_action_binding', default=None)


@contextmanager
def bind_lance_action(before_execute, *, max_rows_per_batch):
    """Internal registration hook, before any worker/source activation.

    No public Dataset option, new action, scheduler or commit authority lives
    here. The hook receives the already-created original RunAction once.
    """
    if _BINDING.get() is not None:
        raise RuntimeError('Nested Lance registration binding')
    token = _BINDING.set((before_execute, max_rows_per_batch))
    try:
        yield
    finally:
        _BINDING.reset(token)


def _lance_action_binding():
    """Read the existing registration without exposing it through SDK requests."""
    return _BINDING.get()


def _consume_lance_action_binding(binding, action, spec):
    # Clear before the callback: a nested write must not bind a second action.
    _BINDING.set(None)
    binding[0](action, spec)
