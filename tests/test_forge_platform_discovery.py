import json
import os
from pathlib import Path
import subprocess
import sys
import types

import pytest

from demiflow.forge.platform import discover_catalog, inspect_modules, limits_with, module_scope
from demiflow.forge.platform_worker import capture
from demiflow.forge.schema import ForgeError


@pytest.fixture
def native(tmp_path, monkeypatch):
    module = types.ModuleType("demiflow.fixture")
    module.__file__ = str(tmp_path / "fixture.py")
    Path(module.__file__).touch()
    exec('''
class Dangerous:
    def __repr__(self):
        raise AssertionError("repr evaluated")
class Base:
    def inherited(self): pass
class Native(Base):
    """Native class documentation."""
    def __init__(self):
        raise AssertionError("constructor evaluated")
    @property
    def value(self):
        """Read-only value."""
        raise AssertionError("getter evaluated")
    @staticmethod
    def call(a: "do_not_evaluate()", /, b=Dangerous(), *, limit=2, **kwargs):
        """Complete original documentation.\n\nAdditional contract paragraph."""
    @classmethod
    def create(cls, *, flag=False): pass
    def _private(self): pass
Alias = Native
def same_name(): pass
def other(): pass
other.__name__ = "same_name"
__all__ = ["Native", "Alias", "same_name", "other"]
''', vars(module))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module, tmp_path


def inspect_fixture(native, **limits):
    module, root = native
    return inspect_modules([module.__name__], root, limits_with(limits), importer=lambda _: module)


def test_dynamic_discovery_aliases_inheritance_and_no_execution(native):
    result = inspect_fixture(native)
    assert result["inspection_complete"], result["diagnostics"]
    objects = {item["id"].split(":")[1]: item for item in result["objects"]}
    assert "Native._private" not in objects
    assert objects["Native"]["origin"] == objects["Alias"]["origin"]
    assert "same_name" in objects and "other" in objects
    assert objects["Native.inherited"]["member_owner"].endswith(".Base")
    assert objects["Native.value"]["kind"] == "property"
    assert objects["Native.value"]["signature"]["status"] == "not_applicable"
    assert objects["Native.create"]["kind"] == "classmethod"
    call = objects["Native.call"]
    assert "<demiflow.fixture.Dangerous>" in call["signature"]["text"]
    assert "do_not_evaluate()" in call["signature"]["text"]
    assert call["documentation"]["text"].endswith("Additional contract paragraph.")
    assert call["signature"]["parameters"][-1]["required"] is False
    assert call["native_semantics"] == {"status": "unknown"}
    module, _ = native
    exec("def added(self, value=3): pass\nNative.added = added", vars(module))
    newer = inspect_fixture(native)
    assert len(newer["objects"]) == len(result["objects"]) + 2
    assert any(item["id"].endswith("Native.added") for item in newer["objects"])


@pytest.mark.parametrize("limit,value", [("objects", 1), ("members", 1), ("inheritance", 1), ("doc_bytes", 1), ("signature_bytes", 1)])
def test_discovery_budget_cannot_become_success(native, limit, value):
    result = inspect_fixture(native, **{limit: value})
    assert not result["inspection_complete"]
    assert result["diagnostics"]


def test_catalog_bytes_not_truncated_to_success(native):
    with pytest.raises(ForgeError, match="byte budget"):
        inspect_fixture(native, catalog_bytes=8)


def test_metaclass_signature_property_is_not_evaluated(native):
    module, _ = native
    exec('''
class Meta(type):
    @property
    def __signature__(cls):
        raise AssertionError("metaclass descriptor evaluated")
class Custom(metaclass=Meta):
    pass
__all__.append("Custom")
''', vars(module))
    result = inspect_fixture(native)
    assert result["inspection_complete"], result["diagnostics"]
    custom = next(item for item in result["objects"] if item["id"].endswith(":Custom"))
    assert custom["signature"]["status"] == "unavailable"


def test_unresolved_exports_and_module_shadow_are_incomplete(native):
    module, root = native
    module.__all__.append("missing")
    assert not inspect_fixture(native)["inspection_complete"]
    module.__file__ = str(root.parent / "shadow.py")
    assert "shadowed" in inspect_fixture(native)["diagnostics"][0]["detail"]


def test_scope_and_limits_cannot_expand():
    for modules in ([], ["os"], ["demiflow", "demiflow"]):
        with pytest.raises(ForgeError):
            module_scope(modules)
    for limits in ({"timeout_seconds": 61}, {"objects": True}, {"arbitrary": 1}):
        with pytest.raises(ForgeError):
            limits_with(limits)


def test_real_catalog_and_native_parsers_are_available():
    catalog = discover_catalog()
    assert catalog["inspection_complete"], catalog["diagnostics"]
    ids = {item["id"] for item in catalog["objects"]}
    assert {"demiflow.data:Dataset.map_prompt_async", "demiflow.data:Dataset.agentmap_async",
            "demiflow.agent:load_agent_config", "demiflow.services:DeploymentService"} <= ids
    assert any("load_prompt" in item for item in ids)


def test_actual_imports_do_not_start_services_or_network(tmp_path):
    # Probe the trusted namespaces, not just a fake class. Any attempted
    # subprocess launch or network connection fails the isolated probe.
    script = '''
import sys, socket, subprocess, os
sys.path.insert(0, sys.argv[1])
def forbidden(*a, **kw): raise AssertionError("execution during discovery")
socket.socket.connect = forbidden
socket.socket.connect_ex = forbidden
socket.create_connection = forbidden
subprocess.Popen = forbidden
os.system = forbidden
from pathlib import Path
from demiflow.forge.platform import inspect_modules, PUBLIC_MODULES, LIMITS
result = inspect_modules(PUBLIC_MODULES, Path(sys.argv[1]) / "demiflow", LIMITS)
assert result["inspection_complete"], result["diagnostics"]
'''
    result = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(Path(__file__).resolve().parents[1])],
                            cwd=tmp_path, capture_output=True, timeout=60,
                            env={"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "CUDA_VISIBLE_DEVICES": ""})
    assert result.returncode == 0, result.stderr.decode()


def test_worker_ignores_project_path_and_secrets(tmp_path, monkeypatch):
    (tmp_path / "demiflow.py").write_text('raise AssertionError("shadow imported")')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    monkeypatch.setenv("FORGE_TEST_SECRET", "must-not-pass")
    assert discover_catalog(["demiflow.agent"])["inspection_complete"]
    result = capture([sys.executable, "-I", "-B", "-c",
                       'import os,json; print(json.dumps({"format":"demiflow_forge_platform_catalog_v1", "secret":os.getenv("FORGE_TEST_SECRET")}))'], {}, limits_with())
    assert result["secret"] is None


def test_capture_does_not_import_the_workbench_layer(tmp_path):
    script = '''import json, sys
sys.path.insert(0, sys.argv[1])
from demiflow.forge.platform import limits_with
from demiflow.forge.platform_worker import capture
assert not any(n == 'demiflow.workbench' or n.startswith('demiflow.workbench.') for n in sys.modules)
code = 'import json;print(json.dumps({"format":"demiflow_forge_platform_catalog_v1"}))'
assert capture([sys.executable, '-I', '-B', '-c', code], {}, limits_with())['format'] == 'demiflow_forge_platform_catalog_v1'
assert not any(n == 'demiflow.workbench' or n.startswith('demiflow.workbench.') for n in sys.modules)
'''
    result = subprocess.run([sys.executable, '-I', '-B', '-c', script,
        str(Path(__file__).resolve().parents[1])], cwd=tmp_path, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr.decode()


def test_native_thread_allocations_do_not_reserve_an_arena_per_thread():
    # Reproduce native-thread allocator reservations without loading a large
    # table. The allocations total 8 MiB; default glibc arenas can reserve
    # hundreds of MiB and exhaust the preview worker's address-space budget.
    code = '''
import ctypes, json, os, threading
libc = ctypes.CDLL(None)
libc.malloc.argtypes = [ctypes.c_size_t]
libc.malloc.restype = ctypes.c_void_p
libc.free.argtypes = [ctypes.c_void_p]
threading.stack_size(256 * 1024)
def virtual_bytes():
    with open('/proc/self/statm') as stream:
        return int(stream.read(128).split()[0]) * os.sysconf('SC_PAGE_SIZE')
baseline = virtual_bytes()
ready = threading.Barrier(9, timeout=10)
release = threading.Event()
def allocate():
    pointers = []
    try:
        for _ in range(32):
            address = libc.malloc(32768)
            if not address:
                raise MemoryError('native allocation failed')
            pointers.append(address)
            ctypes.memset(address, 1, 32768)
        ready.wait()
        assert release.wait(10)
    finally:
        for address in pointers:
            libc.free(address)
threads = [threading.Thread(target=allocate) for _ in range(8)]
try:
    for thread in threads:
        thread.start()
    ready.wait()
    growth = virtual_bytes() - baseline
finally:
    release.set()
    for thread in threads:
        if thread.ident is not None:
            thread.join(10)
print(json.dumps({'format': 'demiflow_forge_platform_catalog_v1', 'growth': growth}))
'''
    result = capture([sys.executable, "-I", "-B", "-c", code], {}, limits_with())
    assert result["growth"] < 128 * 1024**2


@pytest.mark.parametrize("code,limits,message", [
    ("import time; time.sleep(10)", {"timeout_seconds": 1}, "timed out"),
    ('print("x"*4096)', {"catalog_bytes": 128}, "stdout exceeds"),
    ('import sys; sys.stderr.write("x"*4096)', {"stderr_bytes": 128}, "stderr exceeds"),
    ('print("not json")', {}, "Invalid platform inspection"),
])
def test_worker_failure_is_bounded(code, limits, message):
    with pytest.raises(ForgeError, match=message):
        capture([sys.executable, "-I", "-B", "-c", code], {}, limits_with(limits))


def test_timeout_reaps_its_process_group(tmp_path):
    marker = tmp_path / "should-not-exist"
    child = f"import time,pathlib; time.sleep(2); pathlib.Path({str(marker)!r}).touch()"
    parent = f"import subprocess,sys,time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(10)"
    with pytest.raises(ForgeError, match="timed out"):
        capture([sys.executable, "-I", "-B", "-c", parent], {}, limits_with({"timeout_seconds": 1}))
    # Child would publish after the timeout if it survived group cleanup.
    import time
    time.sleep(1.3)
    assert not marker.exists()


def test_sigterm_cleans_worker_and_restores_handler(tmp_path):
    import signal
    import time

    ready = tmp_path / "ready"
    marker = tmp_path / "should-not-exist"
    child = f"import pathlib,time; pathlib.Path({str(ready)!r}).touch(); time.sleep(2); pathlib.Path({str(marker)!r}).touch()"
    parent = f'''
import signal, sys
from demiflow.forge.platform import limits_with
from demiflow.forge.platform_worker import capture
previous = signal.getsignal(signal.SIGTERM)
try:
    capture([sys.executable, "-B", "-c", {child!r}], {{}}, limits_with())
except RuntimeError as exc:
    assert str(exc) == "Platform inspection cancelled"
    assert signal.getsignal(signal.SIGTERM) is previous
    raise SystemExit(42)
raise AssertionError("cancellation was ignored")
'''
    process = subprocess.Popen([sys.executable, "-B", "-c", parent], cwd=Path(__file__).resolve().parents[1], start_new_session=True)
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(.01)
        assert ready.exists()
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 42
        time.sleep(2.1)
        assert not marker.exists()
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def test_worker_parent_startup_race_is_refused(tmp_path):
    from demiflow.forge.platform_worker import bind_parent
    import os
    old=os.environ.get('DEMIFLOW_INSPECTION_PARENT')
    os.environ['DEMIFLOW_INSPECTION_PARENT']='1'
    try:
        with pytest.raises(RuntimeError,match='parent exited'):
            bind_parent()
    finally:
        if old is None:os.environ.pop('DEMIFLOW_INSPECTION_PARENT',None)
        else:os.environ['DEMIFLOW_INSPECTION_PARENT']=old


def test_parent_exit_kills_worker_in_separate_session(tmp_path):
    import os,signal,time
    ready=tmp_path/'ready';marker=tmp_path/'survived'
    child=f'''from demiflow.forge.platform_worker import bind_parent
import pathlib,time,os
bind_parent()
pathlib.Path({str(ready)!r}).write_text(str(os.getpid()))
time.sleep(2)
pathlib.Path({str(marker)!r}).touch()
'''
    parent=f'''import subprocess,sys,os,time
subprocess.Popen([sys.executable,'-B','-c',{child!r}],env={{**os.environ,'DEMIFLOW_INSPECTION_PARENT':str(os.getpid())}},start_new_session=True)
time.sleep(30)
'''
    process=subprocess.Popen([sys.executable,'-B','-c',parent],cwd=Path(__file__).resolve().parents[1])
    try:
        deadline=time.monotonic()+10
        while not ready.exists() and time.monotonic()<deadline:
            assert process.poll() is None
            time.sleep(.02)
        assert ready.exists()
        process.kill();process.wait(timeout=3)
        time.sleep(2.1)
        assert not marker.exists()
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=3)
        if ready.exists():
            try:os.kill(int(ready.read_text()),signal.SIGKILL)
            except ProcessLookupError:pass


def test_untrusted_module_getattr_is_never_called(native):
    module, _ = native
    def forbidden(name):
        raise AssertionError("arbitrary module getter evaluated")
    module.__getattr__ = forbidden
    module.__all__.append("missing")
    assert not inspect_fixture(native)["inspection_complete"]


def test_owned_lazy_facades_cannot_resolve_external_targets(monkeypatch):
    import demiflow
    from demiflow.forge.platform import _public_export
    root = Path(demiflow.__file__).parent
    facade = types.ModuleType("demiflow.data")
    facade.__file__ = str(root / "data/__init__.py")
    facade.__all__ = ["public"]
    facade._EXPORTS = {"public": ("os", "getpid")}
    facade.__getattr__ = lambda name: (_ for _ in ()).throw(AssertionError("getter called"))
    with pytest.raises(ForgeError, match="outside the platform"):
        _public_export(facade, "public", root)
    facade._EXPORTS = {"public": ("." * 257, "getpid")}
    with pytest.raises(ForgeError, match="Invalid lazy"):
        _public_export(facade, "public", root)
