from concurrent.futures import ThreadPoolExecutor
import threading
import time

import pytest

from demiflow.services.triton_dispatch import guard_autotuner


def test_shared_arguments_are_isolated_and_exception_releases_lock():
    class Autotuner:
        def run(self, value):
            self.nargs = value
            time.sleep(.002)  # Releases the GIL, like compilation/launch hooks.
            if value == -1: raise ValueError('launch failed')
            result = self.nargs
            self.nargs = None
            return result
    assert guard_autotuner(Autotuner)
    guarded = Autotuner.run
    assert guard_autotuner(Autotuner) and Autotuner.run is guarded
    kernel = Autotuner()
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(kernel.run,range(40))) == list(range(40))
    with pytest.raises(ValueError,match='launch failed'): kernel.run(-1)
    assert kernel.run(41) == 41


def test_different_kernels_overlap_and_dispatch_does_not_wait_for_gpu_completion():
    barrier = threading.Barrier(2)
    completion = threading.Event()
    class Autotuner:
        def run(self, value):
            self.nargs = value
            barrier.wait(timeout=3)
            # Model an asynchronous launch: completion is deliberately pending.
            return completion
    assert guard_autotuner(Autotuner)
    first, second = Autotuner(), Autotuner()
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(kernel.run,i) for i,kernel in enumerate((first,second))]
        assert all(f.result(timeout=4) is completion for f in futures)
    assert not completion.is_set()
    assert first._demiflow_dispatch_lock is not second._demiflow_dispatch_lock


def test_unknown_implementation_is_not_rewritten():
    class Autotuner:
        def run(self,value): return value
    original = Autotuner.run
    assert guard_autotuner(Autotuner) is False
    assert Autotuner.run is original
