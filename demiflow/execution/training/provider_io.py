"""Deadline-aware transport calls; only the driver may mutate durable fit state."""
from concurrent.futures import Future
import inspect
import threading
import time


# No task queue. An uncooperative vendor transport retains a slot until actual
# exit. It can neither strand interpreter shutdown nor create unbounded retries.
_TRANSPORT_LIMIT = threading.BoundedSemaphore(16)


def check_deadlines(provider, methods):
    for name in methods:
        method = getattr(provider, name, None)
        if not callable(method):
            raise ValueError("Missing provider operation " + name)
        try:
            inspect.signature(method).bind_partial(deadline=0.0)
        except (ValueError, TypeError) as exc:
            raise ValueError("Provider operation must accept keyword-only deadline: " + name) from exc


def _invoke(completion, method, args, deadline):
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError("Provider operation expired before transport")
        completion.set_result(method(*args, deadline=deadline))
    except BaseException as exc:
        completion.set_exception(exc)
    finally:
        _TRANSPORT_LIMIT.release()


class ProviderCalls:
    def __init__(self, provider, timeout_s):
        self.provider = provider
        self.timeout_s = timeout_s
        self.outstanding = None

    def call(self, method, args, run_deadline, *, before_start=None):
        deadline = min(time.monotonic() + self.timeout_s, run_deadline)
        while self.outstanding is not None and not self.outstanding.done():
            if time.monotonic() >= deadline:
                raise TimeoutError("Original provider call has not returned")
            yield
        while not _TRANSPORT_LIMIT.acquire(blocking=False):
            if time.monotonic() >= deadline:
                raise TimeoutError("Training provider transport capacity exhausted")
            yield
        completion = Future()
        self.outstanding = completion
        try:
            if before_start is not None:
                before_start()  # Driver thread, after capacity and immediately before transport.
            threading.Thread(name='demiflow-fit-provider', daemon=True, target=_invoke,
                             args=(completion, getattr(self.provider,method), args, deadline)).start()
        except BaseException:
            _TRANSPORT_LIMIT.release()
            self.outstanding = None
            raise
        while not completion.done():
            if time.monotonic() >= deadline:
                raise TimeoutError("Training provider operation exceeded its deadline")
            yield
        return completion.result()
