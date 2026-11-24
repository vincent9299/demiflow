"""S013 C04 identity counter-examples (reverses the retired D2 assertions).

Q12 (RESPONSE.md:61) binds: a plain callable's initial_state must NOT disappear
for lack of hooks, and a ContextVar's business value must NOT be erased to a type
name. These pins prove the four C04 identity rules and the rule-5 collision
counter-examples for the newly admitted synchronization-resource declaration:

  rule 1  hookless + hooked callables alike fold __dict__ config into identity;
  rule 2  thresholds 0.5/0.9 and different ContextVar values/names never collide;
  rule 3  only proven no-semantics sync primitives get a stable type declaration
          (Thread / thread-local / opaque handles are NOT swallowed by type);
  rule 4  unadapted opaque deps fail closed, never silently type-erased;
  rule 5  sharing a resource type does not collide distinct business configs, and
          the accepted historical actor+initial_state shape is byte-stable.
"""
import contextvars
import functools
import json
import os
from pathlib import Path
import subprocess
import sys
import threading

import pytest
import demiflow

from demiflow.execution import RecoveryContractError
from demiflow.execution.seal.program_identity import describe
from demiflow.execution.contracts.recovery_contract import document, identity


class Hooked:
    def __init__(self, threshold):
        self.threshold = threshold

    def __call__(self, row):
        return row

    def snapshot_state(self):
        return {"threshold": self.threshold}

    def restore_state(self, state):
        self.threshold = state["threshold"]


class Hookless:
    def __init__(self, threshold):
        self.threshold = threshold

    def __call__(self, row):
        return row


class EmptyHookless:
    def __call__(self, row):
        return row


# --- rule 1: initial_state survives for hookless callables, configs differ -----

def test_hookless_callable_retains_initial_state_and_distinguishes_config():
    low, high, low_again = Hookless(0.5), Hookless(0.9), Hookless(0.5)
    assert "initial_state" in describe(low)
    assert describe(low)["initial_state"] == {"threshold": 0.5}
    assert describe(low) != describe(high)
    assert identity(describe(low)) != identity(describe(high))
    assert describe(low) == describe(low_again)


def test_hooked_callable_identity_shape_is_historical():
    actor = Hooked(0.5)
    described = describe(actor)
    assert set(described) == {"actor", "initial_state"}
    assert described["initial_state"] == {"threshold": 0.5}
    assert identity(described) != identity(describe(Hooked(0.9)))


def test_empty_hookless_callable_is_stable_and_documentable():
    a, b = EmptyHookless(), EmptyHookless()
    assert a.__dict__ == {}
    described = describe(a)
    assert described["initial_state"] == {}
    assert described == describe(b)
    assert isinstance(document(described), bytes) and document(described)
    assert identity(described) == identity(describe(b))


def test_partial_is_actor_with_empty_initial_state():
    part = functools.partial(lambda x: x, 1)
    described = describe(part)
    assert set(described) == {"actor", "initial_state"}
    assert described["initial_state"] == {}


# --- rule 2: ContextVar business value and name must differ --------------------

def test_contextvar_value_and_name_are_distinguished():
    base = contextvars.ContextVar("worker", default=0)
    other_value = contextvars.ContextVar("worker", default=9)
    other_name = contextvars.ContextVar("other", default=0)
    assert describe(base) == {"contextvar": "worker", "value": 0}
    assert identity(describe(base)) != identity(describe(other_value))
    assert identity(describe(base)) != identity(describe(other_name))
    assert isinstance(document(describe(base)), bytes)


def test_contextvar_without_default_is_stable():
    var = contextvars.ContextVar("no-default")
    assert describe(var) == {"contextvar": "no-default", "value": None}


def test_threshold_configs_never_collide():
    assert identity(describe(Hookless(0.5))) != identity(describe(Hookless(0.9)))


# --- rule 3: only proven sync primitives get a stable type declaration ---------

@pytest.mark.parametrize("factory,expected", [
    (threading.Lock, "_thread.lock"),
    (threading.RLock, "_thread.RLock"),
    (threading.Event, "threading.Event"),
    (lambda: threading.Barrier(2), "threading.Barrier"),
    (threading.Semaphore, "threading.Semaphore"),
    (threading.BoundedSemaphore, "threading.BoundedSemaphore"),
    (lambda: threading.Condition(), "threading.Condition"),
])
def test_sync_primitives_get_stable_address_free_declaration(factory, expected):
    descriptor = describe(factory())
    assert descriptor == {"runtime_resource": expected}
    assert isinstance(document(descriptor), bytes)
    assert "0x" not in document(descriptor).decode()


def test_thread_and_thread_local_are_not_swallowed_by_type():
    with pytest.raises(RecoveryContractError, match="No native semantic identity"):
        describe(threading.Thread())
    with pytest.raises(RecoveryContractError, match="No native semantic identity"):
        describe(threading.local())


# --- rule 4: unadapted opaque deps fail closed ---------------------------------

def test_unadapted_noncallable_object_still_fails_closed():
    class Opaque:
        pass

    with pytest.raises(RecoveryContractError, match="No native semantic identity"):
        describe(Opaque())
    with pytest.raises(RecoveryContractError, match="No native semantic identity"):
        describe({Opaque()})


def test_hookless_actor_with_unadapted_dep_fails_closed():
    class NoCopy:
        def __deepcopy__(self, memo):
            raise RuntimeError("must not deepcopy")

    class Stage:
        def __init__(self):
            self.dep = NoCopy()
            self.lock = threading.Lock()

        def __call__(self, row):
            return row

    # The lock is a proven sync primitive, but the opaque dep is unadapted: the
    # durable identity must fail closed rather than erase the dep (retired D2
    # behaviour silently dropped the whole initial_state).
    with pytest.raises(RecoveryContractError, match="No native semantic identity"):
        describe(Stage())


# --- rule 5: collision counter-examples for the admitted resource declaration --

def test_shared_resource_type_does_not_collide_business_config():
    class WithLock:
        def __init__(self, threshold):
            self.threshold = threshold
            self.lock = threading.Lock()

        def __call__(self, row):
            return row

    low, high = WithLock(0.5), WithLock(0.9)
    assert describe(low)["initial_state"]["lock"] == {"runtime_resource": "_thread.lock"}
    assert describe(high)["initial_state"]["lock"] == {"runtime_resource": "_thread.lock"}
    # Same lock type, different business config => identities MUST differ.
    assert identity(describe(low)) != identity(describe(high))


def make_worker():
    lock = threading.Lock()
    event = threading.Event()
    var = contextvars.ContextVar("worker", default=0)

    def worker(row):
        with lock:
            event.set()
            var.set(var.get() + 1)
        return row

    return worker


def test_closure_resources_stable_address_free_and_value_bearing():
    id1, id2 = describe(make_worker()), describe(make_worker())
    assert id1 == id2
    encoded = document(id1)
    assert isinstance(encoded, bytes) and encoded
    assert "0x" not in encoded.decode()
    resources = [c for c in id1["closure"] if isinstance(c, dict) and "runtime_resource" in c]
    assert {r["runtime_resource"] for r in resources} == {"_thread.lock", "threading.Event"}
    context = [c for c in id1["closure"] if isinstance(c, dict) and "contextvar" in c]
    assert context == [{"contextvar": "worker", "value": 0}]


def test_closure_resources_cross_process_consistent():
    root = Path(demiflow.__file__).resolve().parents[1]
    script = '''import json,os,threading,contextvars
from pathlib import Path
import demiflow
from demiflow.execution.seal.program_identity import describe
assert Path(demiflow.__file__).resolve().is_relative_to(Path(os.environ['DF_INTEGRATION_ROOT']))
def make_worker():
    lock=threading.Lock(); event=threading.Event(); var=contextvars.ContextVar("worker",default=0)
    def worker(row):
        with lock:
            event.set(); var.set(var.get()+1)
        return row
    return worker
print(json.dumps(describe(make_worker()),sort_keys=True))
'''
    results = []
    for seed in ["1", "999"]:
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(root), DF_INTEGRATION_ROOT=str(root))
        run = subprocess.run([sys.executable, "-B", "-c", script], cwd=root,
                             env=env, capture_output=True, text=True, timeout=30)
        assert run.returncode == 0, run.stderr
        results.append(json.loads(run.stdout))
    assert results[0] == results[1]
    assert "0x" not in json.dumps(results[0])
    resources = [c for c in results[0]["closure"] if isinstance(c, dict) and "runtime_resource" in c]
    assert {r["runtime_resource"] for r in resources} == {"_thread.lock", "threading.Event"}
    context = [c for c in results[0]["closure"] if isinstance(c, dict) and "contextvar" in c]
    assert context == [{"contextvar": "worker", "value": 0}]