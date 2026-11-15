"""Diagnostic run only: timeouts fail explicitly, never skip/pass a test."""
import signal
from contextlib import contextmanager
import pytest

@contextmanager
def deadline(nodeid, phase):
    def expired(signum, frame):
        raise TimeoutError(f'REVIEW_DIAGNOSTIC_TIMEOUT: {nodeid} {phase} exceeded 15 seconds')
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, 15, 15)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item):
    with deadline(item.nodeid, 'setup'): yield

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    with deadline(item.nodeid, 'call'): yield

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item):
    with deadline(item.nodeid, 'teardown'): yield
