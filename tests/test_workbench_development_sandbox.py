"""Actual Linux adversarial fixtures. No accounts, business ACLs or models."""
import dataclasses
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

import pytest

from demiflow.resources.process_observation import descendants
from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox
from demiflow.workbench.conversations.execution import launch_provider

PYTHON = '/usr/bin/python3'


@pytest.fixture
def scope(tmp_path):
    home = tmp_path / 'home'; home.mkdir(mode=0o700)
    source = tmp_path / 'source'; source.mkdir()
    readable = tmp_path / 'readonly'; readable.mkdir()
    (readable / 'file').write_text('readable-preserved')
    secret = tmp_path / 'control'; secret.mkdir()
    (secret / 'receipt').write_text('authoritative-preserved')
    spec = SandboxSpec(read_paths=(str(readable),), write_paths=(str(source),),
        runtime_paths=('/usr', '/lib', '/lib64'), protected_paths=(str(readable),),
        denied_paths=(str(secret),), private_home=str(home), seconds=10)
    return spec, source, readable, secret


def run_code(spec, cwd, code, *args, **kwargs):
    process = launch_sandbox([PYTHON, '-I', '-B', '-c', code, *map(str, args)],
        spec=spec, env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, cwd=cwd, **kwargs)
    try:
        stdout, stderr = process.communicate(timeout=12)
    finally:
        if process.poll() is None:
            process.kill(); process.communicate(timeout=5)
    assert process.returncode == 0, (process.returncode, stderr.decode())
    return json.loads(stdout)


def test_real_file_syscall_process_and_network_denials_inherit(scope, tmp_path):
    spec, source, readable, secret = scope
    listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen()
    code = r'''
import ctypes,errno,json,os,pathlib,socket,sys
write,read,control,hostpid,port=sys.argv[1:]
out={}
def attempt(name,fn):
    try: fn();out[name]='ALLOWED'
    except OSError as error:out[name]=error.errno
libc=ctypes.CDLL(None,use_errno=True)
def syscall(number,*args):
    result=libc.syscall(number,*args)
    if result<0:raise OSError(ctypes.get_errno(),'denied')
pathlib.Path(write,'ok').write_text('allowed')
out['read']=pathlib.Path(read,'file').read_text()
attempt('control_read',lambda:pathlib.Path(control,'receipt').read_text())
attempt('control_write',lambda:pathlib.Path(control,'receipt').write_text('bad'))
attempt('readonly_write',lambda:pathlib.Path(read,'file').write_text('bad'))
attempt('readonly_hardlink',lambda:os.link(pathlib.Path(read,'file'),pathlib.Path(write,'alias')))
attempt('control_hardlink',lambda:os.link(pathlib.Path(control,'receipt'),pathlib.Path(write,'control-alias')))
attempt('readonly_truncate',lambda:os.truncate(pathlib.Path(read,'file'),0))
attempt('readonly_open_truncate',lambda:os.close(os.open(pathlib.Path(read,'file'),os.O_RDONLY|os.O_TRUNC)))
attempt('chmod',lambda:os.chmod(pathlib.Path(read,'file'),0o777))
attempt('chown',lambda:os.chown(pathlib.Path(read,'file'),0,0))
attempt('utime',lambda:os.utime(pathlib.Path(read,'file'),(0,0)))
attempt('xattr',lambda:os.setxattr(pathlib.Path(read,'file'),'user.bad',b'bad'))
attempt('proc_secret',lambda:pathlib.Path('/proc',hostpid,'environ').read_bytes())
attempt('proc_fd',lambda:pathlib.Path('/proc',hostpid,'fd','0').read_bytes())
attempt('proc_root',lambda:pathlib.Path('/proc',hostpid,'root','etc','passwd').read_bytes())
attempt('unshare',lambda:syscall(272,0x10000000))
attempt('setns',lambda:syscall(308,-1,0))
attempt('clone3',lambda:syscall(435,0,0))
attempt('ptrace',lambda:syscall(101,16,int(hostpid),0,0))
attempt('process_vm_readv',lambda:syscall(310,int(hostpid),0,0,0,0,0))
attempt('io_uring',lambda:syscall(425,1,0))
attempt('openat2',lambda:syscall(437,-100,b'/etc/passwd',0,0))
attempt('unix_socket',lambda:socket.socket(socket.AF_UNIX,socket.SOCK_STREAM))
attempt('raw_socket',lambda:socket.socket(socket.AF_INET,socket.SOCK_RAW,1))
attempt('host_loopback',lambda:socket.create_connection(('127.0.0.1',int(port)),.2))
attempt('signal_host',lambda:os.kill(int(hostpid),0))
child=os.fork()
if not child:
    try:pathlib.Path(control,'receipt').write_text('bad-child');os._exit(5)
    except OSError:os._exit(0)
out['child_denied']=os.waitstatus_to_exitcode(os.waitpid(child,0)[1])==0
print(json.dumps(out))
'''
    try:
        result = run_code(spec, source, code, source, readable, secret, os.getpid(), listener.getsockname()[1])
    finally:
        listener.close()
    assert result.pop('read') == 'readable-preserved'
    assert result.pop('child_denied') is True
    assert result and all(type(value) is int and value != 0 for value in result.values()), json.dumps(result)
    assert (source / 'ok').read_text() == 'allowed'
    assert (readable / 'file').read_text() == 'readable-preserved'
    assert (secret / 'receipt').read_text() == 'authoritative-preserved'


def test_capabilities_and_control_descriptors_are_not_inherited(scope, tmp_path):
    spec, source, _, _ = scope
    with (tmp_path / 'control-fd').open('w+b') as control:
        os.set_inheritable(control.fileno(), True)
        process = launch_sandbox([PYTHON, '-I', '-B', '-c', 'import time;print("ready",flush=True);time.sleep(5)'],
            spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=source, lease_fd=control.fileno())
        try:
            assert process.stdout.readline() == b'ready\n'
            rows = descendants(process.pid)
            pid1 = next(pid for pid in rows if '\t1\n' in Path(f'/proc/{pid}/status').read_text().split('NSpid:', 1)[1].split('NSpgid:', 1)[0])
            children = descendants(pid1)
            assert children
            for pid in children:
                status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines())
                assert all(int(status[name].strip(), 16) == 0 for name in ('CapEff', 'CapPrm', 'CapInh', 'CapBnd', 'CapAmb'))
                assert status['NoNewPrivs'].strip() == '1'
                unit = os.fstat(control.fileno())
                for fd in Path(f'/proc/{pid}/fd').iterdir():
                    try: info = fd.stat()
                    except FileNotFoundError: continue
                    assert (info.st_dev, info.st_ino) != (unit.st_dev, unit.st_ino)
        finally:
            process.kill(); process.communicate(timeout=5)


@pytest.mark.parametrize('stop', ['kill_launcher', 'deadline', 'leader_exit'])
def test_namespace_domain_stops_detached_descendants(scope, stop):
    spec, source, _, _ = scope
    spec = dataclasses.replace(spec, seconds=1 if stop == 'deadline' else 10)
    code = '''import os,time
pid=os.fork()
if pid==0:
 os.setsid()
 while True:time.sleep(1)
print('ready',flush=True)
time.sleep(.25 if %r=='leader_exit' else 30)
''' % stop
    process = launch_sandbox([PYTHON, '-I', '-B', '-c', code],spec=spec,
        env={'PATH': '/usr/bin:/bin'}, cwd=source)
    try:
        assert process.stdout.readline() == b'ready\n'
        owned = descendants(process.pid)
        assert len(owned) >= 3
        if stop == 'kill_launcher': process.kill()
        process.communicate(timeout=4)
        until = time.monotonic() + 2
        while time.monotonic() < until:
            live = []
            for pid in owned:
                try:
                    state = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[0]
                    if state != 'Z': live.append(pid)
                except (FileNotFoundError, ProcessLookupError):
                    pass
            if not live: break
            time.sleep(.03)
        assert not live
    finally:
        if process.poll() is None:
            process.kill(); process.communicate(timeout=5)


def test_only_controller_fixed_channel_is_reachable(scope, tmp_path):
    spec, source, _, _ = scope
    target = tmp_path / 'control.sock'
    server = socket.socket(socket.AF_UNIX); server.bind(str(target)); server.listen()
    def respond():
        for _ in range(2):
            connection, _ = server.accept()
            with connection:
                request = connection.recv(1024)
                connection.sendall(b'fixed:' + request)
    thread = threading.Thread(target=respond, daemon=True); thread.start()
    code = '''import json,os,socket
host,port=os.environ['DEMIFLOW_CONTROL_ENDPOINT'].split(':')
out=[]
for value in (b'one',b'two'):
 with socket.create_connection((host,int(port))) as stream:
  stream.sendall(value);out.append(stream.recv(1024).decode())
print(json.dumps(out))
'''
    try:
        result = run_code(spec, source, code, control_socket=str(target))
        assert result == ['fixed:one', 'fixed:two']
    finally:
        server.close(); thread.join(timeout=1)


def test_protected_subtrees_and_readonly_source_writes_fail_before_exec(scope):
    spec, source, readable, secret = scope
    marker = source / 'never'
    command = [PYTHON, '-c', 'from pathlib import Path;Path(%r).touch()' % str(marker)]
    for invalid in (dataclasses.replace(spec, write_paths=(str(readable),)),
                    dataclasses.replace(spec, read_paths=(str(secret.parent),)),
                    dataclasses.replace(spec, role='readonly')):
        with pytest.raises(ValueError):
            launch_sandbox(command,spec=invalid,env={},cwd=source)
    assert not marker.exists()


@pytest.mark.parametrize('role', ['readonly', 'verifier'])
def test_readonly_roles_allow_only_private_scratch_writes(scope, role):
    spec, source, readable, _ = scope
    spec = dataclasses.replace(spec, role=role, write_paths=(), read_paths=(str(source), str(readable)))
    code = '''import json,os,pathlib,sys
pathlib.Path(os.environ['HOME'],'result').write_text('scratch')
try:pathlib.Path(sys.argv[1],'new').write_text('bad')
except OSError as error:print(json.dumps({'denied':error.errno}))
else:raise RuntimeError('Read-only role wrote source')
'''
    assert run_code(spec, source, code, source)['denied'] == 13
    assert not (source / 'new').exists()
    assert (Path(spec.private_home) / 'result').read_text() == 'scratch'


def test_untrusted_home_link_poison_is_rejected_before_trusted_adapter(scope, tmp_path):
    spec, source, _, secret = scope
    (Path(spec.private_home) / 'dsh').symlink_to(secret, target_is_directory=True)
    calls = []
    class Adapter:
        @staticmethod
        def launch(*args):
            calls.append(True)
            raise AssertionError('Poisoned HOME reached trusted profile preparation')
    with (tmp_path / 'unit').open('w+b') as lease:
        with pytest.raises(ValueError, match='link alias'):
            launch_provider(mode='restricted', adapter=Adapter, config={},
                home=spec.private_home, root=str(source), lease_fd=lease.fileno(), sandbox_spec=spec)
    assert not calls
    assert (secret / 'receipt').read_text() == 'authoritative-preserved'


def test_private_disk_budget_terminates_the_owned_domain(scope):
    spec, source, _, _ = scope
    spec = dataclasses.replace(spec, private_bytes=1024)
    code = 'import os,pathlib,time;pathlib.Path(os.environ["HOME"],"large").write_bytes(b"x"*2048);time.sleep(30)'
    started = time.monotonic()
    process = launch_sandbox([PYTHON, '-I', '-B', '-c', code],spec=spec,env={},cwd=source)
    try:
        process.communicate(timeout=4)
        assert process.returncode == 125
        assert time.monotonic() - started < 3
    finally:
        if process.poll() is None:
            process.kill();process.communicate(timeout=5)


def test_model_placeholder_requires_actual_fixed_channel(scope):
    spec, source, _, _ = scope
    with pytest.raises(RuntimeError, match='requires the fixed model channel'):
        launch_sandbox([PYTHON, '-c', 'raise RuntimeError("never exec")', '__DEMIFLOW_MODEL_ENDPOINT__'],
            spec=spec, env={}, cwd=source)


def test_execution_receipt_is_actual_launch_unique_and_version_bound(scope):
    from demiflow.workbench.conversations.sandbox import execution_identity
    spec, source, _, _ = scope
    identities = []
    for _ in range(2):
        process = launch_sandbox([PYTHON, '-c', 'pass'],spec=spec,env={},cwd=source)
        process.communicate(timeout=4)
        assert process.returncode == 0
        assert process.execution_assurance == 'enforced'
        assert {key:process.execution_evidence[key] for key in ('backend','implementation')} == execution_identity()
        identities.append(process.execution_evidence['execution_id'])
    assert identities[0] != identities[1]


def test_compat_syscall_abis_cannot_bypass_native_filter(scope):
    spec, source, _, _ = scope
    code = r'''import ctypes,json,mmap,os
libc=ctypes.CDLL(None,use_errno=True)
out=[]
for abi in ('x32','i386'):
 child=os.fork()
 if child==0:
  if abi=='x32':value=libc.syscall(0x40000000+39)
  else:
   page=mmap.mmap(-1,4096,prot=mmap.PROT_READ|mmap.PROT_WRITE|mmap.PROT_EXEC)
   page.write(bytes.fromhex('b814000000cd80c3'))
   value=ctypes.CFUNCTYPE(ctypes.c_int)(ctypes.addressof(ctypes.c_char.from_buffer(page)))()
  os._exit(42 if value>0 else 0)
 _,status=os.waitpid(child,0)
 out.append(os.waitstatus_to_exitcode(status))
print(json.dumps(out))
'''
    result = run_code(spec, source, code)
    assert len(result) == 2 and all(value != 42 for value in result)


def test_python_script_and_shebang_entrypoints_use_only_owned_fd_ioctl(scope):
    spec, source, readable, _ = scope
    script = readable / 'entry'
    script.write_text('#!' + sys.executable + '\nprint("entry-ok")\n')
    script.chmod(0o755)
    spec = dataclasses.replace(spec, runtime_paths=spec.runtime_paths + (sys.prefix, str(script)))
    for command in ([sys.executable, '-I', '-B', str(script)], [str(script)]):
        process = launch_sandbox(command, spec=spec, env={}, cwd=source)
        out, err = process.communicate(timeout=4)
        assert process.returncode == 0, err.decode()
        assert out == b'entry-ok\n'


def test_verifier_pythonpath_is_physical_and_within_read_scope(scope):
    spec, source, readable, secret = scope
    (readable / 'fixed_module.py').write_text('value=42\n')
    spec = dataclasses.replace(spec, role='verifier', write_paths=())
    command = [PYTHON, '-B', '-c', 'import fixed_module;print(fixed_module.value)']
    process = launch_sandbox(command, spec=spec, env={'PYTHONPATH': str(readable)}, cwd=source)
    out, err = process.communicate(timeout=4)
    assert process.returncode == 0, err.decode()
    assert out == b'42\n'
    for value in ('', str(readable) + ':', str(secret), str(readable / '..' / readable.name)):
        with pytest.raises(ValueError):
            launch_sandbox(command, spec=spec, env={'PYTHONPATH': value}, cwd=source)
    with pytest.raises(ValueError, match='Only fixed verifier'):
        launch_sandbox(command, spec=dataclasses.replace(spec, role='developer'), env={'PYTHONPATH': str(readable)}, cwd=source)


def test_runtime_and_loaded_control_plane_are_never_writable(scope, tmp_path):
    from demiflow.workbench.conversations import sandbox
    spec, source, _, _ = scope
    native = Path(sandbox.__file__).with_name('sandbox_native.py')
    original = native.read_bytes()
    runtime = tmp_path / 'runtime'; runtime.mkdir()
    inner = runtime / 'inner'; inner.mkdir()
    sentinel = inner / 'executable'; sentinel.write_text('preserved')
    # Reject equal scopes, both ancestor directions, and the loaded trusted
    # package even when a caller omits it from its declared runtime paths.
    for write, dependencies in ((runtime, (runtime,)), (runtime, (inner,)), (inner, (runtime,)),
                                 (native, ()), (native.parent, ())):
        invalid = dataclasses.replace(spec, write_paths=(str(write),),
            runtime_paths=spec.runtime_paths + tuple(map(str, dependencies)))
        with pytest.raises(ValueError, match='runtime|protected'):
            launch_sandbox([PYTHON, '-c', 'raise RuntimeError("must not execute")'],
                spec=invalid, env={}, cwd=source)
    assert native.read_bytes() == original
    assert sentinel.read_text() == 'preserved'
    # Omitting dependencies from a caller's runtime declaration cannot make
    # the interpreter/unshare/loader used by the trusted launcher writable.
    for dependency in (sys.executable, sys.prefix, '/usr/bin/unshare', '/lib'):
        invalid = dataclasses.replace(spec, write_paths=(dependency,), runtime_paths=())
        with pytest.raises(ValueError, match='protected'):
            launch_sandbox([PYTHON, '-c', 'raise RuntimeError("must not execute")'],
                spec=invalid, env={}, cwd=source)


def test_source_file_growth_shares_disk_budget_and_preserves_external_history(scope):
    spec, source, _, secret = scope
    (source / 'baseline').write_bytes(b'b' * 4096)
    spec = dataclasses.replace(spec, private_bytes=1024)
    code = '''import pathlib,sys,time
root=pathlib.Path(sys.argv[1])
for i in range(8): (root/str(i)).write_bytes(b'x'*256)
time.sleep(30)
'''
    process = launch_sandbox([PYTHON, '-I', '-B', '-c', code, str(source)],
        spec=spec, env={}, cwd=source)
    try:
        process.communicate(timeout=4)
        assert process.returncode == 125
    finally:
        if process.poll() is None:
            process.kill(); process.communicate(timeout=5)
    assert (source / 'baseline').read_bytes() == b'b' * 4096
    assert (secret / 'receipt').read_text() == 'authoritative-preserved'


@pytest.mark.parametrize('kind', ['unlinked', 'memfd'])
def test_anonymous_open_files_share_domain_disk_budget(scope, kind):
    spec, source, _, secret = scope
    spec = dataclasses.replace(spec, private_bytes=1024)
    code = '''import os,sys,time
if sys.argv[1]=='memfd': fd=os.memfd_create('owned-anonymous')
else:
 fd=os.open(sys.argv[2],os.O_RDWR|os.O_CREAT,0o600)
 os.unlink(sys.argv[2])
os.write(fd,b'x'*2048)
time.sleep(30)
'''
    process = launch_sandbox([PYTHON, '-I', '-B', '-c', code, kind, str(source / 'unlinked')],
        spec=spec, env={}, cwd=source)
    try:
        process.communicate(timeout=4)
        assert process.returncode == 125
    finally:
        if process.poll() is None:
            process.kill(); process.communicate(timeout=5)
    assert (secret / 'receipt').read_text() == 'authoritative-preserved'


@pytest.fixture(scope='module')
def observer():
    # PID 1 normally runs as an isolated script with its own module search path.
    with pytest.MonkeyPatch.context() as isolated:
        isolated.setattr(sys, 'path', list(sys.path))
        from demiflow.workbench.conversations import sandbox_child
    return sandbox_child


def test_fd_observer_rejects_live_process_with_inaccessible_descriptors(observer):
    code = (
        "import ctypes, sys; assert ctypes.CDLL(None).prctl(4, 0, 0, 0, 0) == 0; "
        "print('ready', flush=True); sys.stdin.read(1)"
    )
    with subprocess.Popen([sys.executable, '-I', '-B', '-c', code], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE) as child:
        try:
            assert child.stdout.readline() == b'ready\n'
            try:
                with os.scandir(f'/proc/{child.pid}/fd'):
                    pytest.skip('privileged observer may inspect non-dumpable processes')
            except PermissionError:
                pass
            with pytest.raises(PermissionError):
                observer.sample(child.pid, 1, files=True, deadline=time.monotonic() + 1)
            assert child.poll() is None
        finally:
            child.stdin.close()


def test_fd_observer_confirms_zombie_or_gone_using_same_open_stat(monkeypatch, observer):
    def denied(path):
        raise PermissionError(path)

    with subprocess.Popen([sys.executable, '-I', '-B', '-c', 'import sys; sys.stdin.read(1)'],
                          stdin=subprocess.PIPE) as child:
        with open(f'/proc/{child.pid}/stat', 'rb', buffering=0) as proc_stat:
            monkeypatch.setattr(observer.os, 'scandir', denied)
            child.stdin.close()
            os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
            assert observer._open_process_fds(child.pid, proc_stat, time.monotonic() + 1) is None
            assert child.wait() == 0
            assert observer._open_process_fds(child.pid, proc_stat, time.monotonic() + 1) is None


@pytest.mark.parametrize('state,threads', [('R', 1), ('Z', 2)])
def test_fd_observer_rejects_unconfirmed_exit_and_live_thread_group(monkeypatch, observer, state, threads):
    fields = ['0'] * 22
    fields[0], fields[6], fields[17] = state, '4', str(threads)
    proc_stat = io.BytesIO(('123 (worker) ' + ' '.join(fields)).encode())

    def denied(path):
        raise PermissionError(path)

    monkeypatch.setattr(observer.os, 'scandir', denied)
    with pytest.raises(PermissionError):
        observer._open_process_fds(123, proc_stat, time.monotonic())
