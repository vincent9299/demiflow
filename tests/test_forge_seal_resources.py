"""Resource and identity regressions for native seal verification."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tracemalloc

import pytest

from demiflow.forge.runs import manager as runs

from demiflow.forge import platform_environment as environment
from demiflow.forge.schema import ForgeError


def test_tiny_file_does_not_reserve_the_full_read_budget(tmp_path, record_property):
    path = tmp_path / 'tiny.py'
    raw = b'# sealed platform source\n' * 32
    path.write_bytes(raw)
    # Resolve the lazy io import before measuring the read itself. The separate
    # address-space regression also exercises a fresh interpreter/import.
    assert environment.read_bounded(path, 32 * 1024**2) == raw
    tracemalloc.start()
    try:
        assert environment.read_bounded(path, 32 * 1024**2) == raw
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    record_property('file_bytes', len(raw))
    record_property('budget_bytes', 32 * 1024**2)
    record_property('read_peak_bytes', peak)
    # This filesystem advertises a 1 MiB buffered-file block size. Include
    # that fixed overhead while detecting the former 32 MiB reservation.
    assert peak < 2 * 1024**2, peak


def test_tiny_file_reads_under_less_address_headroom_than_the_budget(tmp_path):
    path = tmp_path / 'tiny.py'
    path.write_bytes(b'fixed seal\n')
    package = Path(environment.__file__).resolve().parents[2]
    code = '''
import os, resource, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from demiflow.forge.platform_environment import read_bounded
baseline = int(Path('/proc/self/statm').read_text().split()[0]) * os.sysconf('SC_PAGE_SIZE')
ceiling = baseline + 8 * 1024**2
resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
assert read_bounded(Path(sys.argv[2]), 32 * 1024**2) == b'fixed seal\\n'
print('bounded-read-complete')
'''
    result = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(package), str(path)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'bounded-read-complete'


@pytest.mark.parametrize('size,maximum', [(0, 0), (1, 1), (257, 257), (257, 32 * 1024**2)])
def test_exact_size_and_empty_files(tmp_path, size, maximum):
    path = tmp_path / 'source'
    raw = b'x' * size
    path.write_bytes(raw)
    assert environment.read_bounded(path, maximum) == raw


@pytest.mark.parametrize('maximum', [-1, 0, 3])
def test_size_admission_rejects_before_reading(tmp_path, maximum):
    path = tmp_path / 'source'
    path.write_bytes(b'1234')
    with pytest.raises(ForgeError) as caught:
        environment.read_bounded(path, maximum)
    assert caught.value.code == 'resource_limit'


@pytest.mark.parametrize('change', ['grow', 'grow_over_budget', 'shrink', 'rewrite', 'replace', 'symlink'])
def test_changed_open_file_or_path_is_rejected(tmp_path, monkeypatch, change):
    path = tmp_path / 'source'
    path.write_bytes(b'initial')
    prior = path.stat()
    original = os.fstat
    captured = False

    def change_after_open(fd):
        nonlocal captured
        info = original(fd)
        if not captured:
            captured = True
            if change == 'grow':
                with path.open('ab') as stream:
                    stream.write(b'+')
            elif change == 'grow_over_budget':
                with path.open('ab') as stream:
                    stream.write(b'+' * 64)
            elif change == 'shrink':
                path.write_bytes(b'i')
            elif change == 'rewrite':
                path.write_bytes(b'changed')
                os.utime(path, ns=(prior.st_atime_ns, prior.st_mtime_ns + 1))
            elif change == 'replace':
                replacement = tmp_path / 'replacement'
                replacement.write_bytes(b'initial')
                replacement.replace(path)
            else:
                saved = tmp_path / 'saved'
                path.rename(saved)
                path.symlink_to(saved)
        return info

    monkeypatch.setattr(os, 'fstat', change_after_open)
    with pytest.raises((ForgeError, OSError)) as caught:
        environment.read_bounded(path, 16)
    if isinstance(caught.value, ForgeError):
        assert caught.value.code in {'conflict', 'resource_limit'}


def test_short_read_without_a_stat_change_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / 'source'
    path.write_bytes(b'initial')
    original = os.fdopen

    class ShortRead:
        def __init__(self, descriptor, mode):
            self.source = original(descriptor, mode)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.source.close()

        def fileno(self):
            return self.source.fileno()

        def read(self, size):
            return self.source.read(1)

    monkeypatch.setattr(os, 'fdopen', ShortRead)
    with pytest.raises(ForgeError) as caught:
        environment.read_bounded(path, 32 * 1024**2)
    assert caught.value.code == 'conflict'


def test_regular_file_and_nofollow_protections(tmp_path):
    path = tmp_path / 'source'
    path.write_bytes(b'fixed')
    link = tmp_path / 'link'
    link.symlink_to(path)
    with pytest.raises(ForgeError, match='Symlink'):
        environment.read_bounded(link, 100)
    directory_link = tmp_path / 'parent'
    directory_link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ForgeError, match='Symlink'):
        environment.read_bounded(directory_link / 'source', 100)
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    with pytest.raises(ForgeError, match='regular file'):
        environment.read_bounded(fifo, 100)


def test_worker_environment_controls_allocator_and_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv('MALLOC_ARENA_MAX', '128')
    monkeypatch.setenv('LD_PRELOAD', '/untrusted')
    monkeypatch.setenv('PYTHONPATH', '/untrusted')
    monkeypatch.setenv('Q2_TEST_TOKEN', 'controlled-test-value')
    worker = runs.worker_environment(tmp_path, credential_names=['Q2_TEST_TOKEN'])
    assert worker['MALLOC_ARENA_MAX'] == '2'
    assert worker['Q2_TEST_TOKEN'] == 'controlled-test-value'
    assert 'LD_PRELOAD' not in worker
    assert worker['PYTHONPATH'] == str(tmp_path / 'runtime') + os.pathsep + str(tmp_path / 'source')
    assert worker['OPENBLAS_NUM_THREADS'] == worker['OMP_NUM_THREADS'] == '1'
    assert worker['RAYON_NUM_THREADS'] == worker['TOKIO_WORKER_THREADS'] == '2'
    assert worker['CUDA_VISIBLE_DEVICES'] == ''
    for name in ('MALLOC_ARENA_MAX', 'LD_PRELOAD', 'LD_LIBRARY_PATH', 'HOME', 'PYTHONPATH', 'DEMIFLOW_TEST'):
        with pytest.raises(ForgeError, match='conflicts'):
            runs.worker_environment(tmp_path, credential_names=[name])
    with pytest.raises(ForgeError, match='unavailable'):
        runs.worker_environment(tmp_path, credential_names=['Q2_MISSING_TOKEN'])


def test_worker_starts_with_allocator_control_and_keeps_allocation_guard(tmp_path):
    package = Path(environment.__file__).resolve().parents[2]
    worker = runs.worker_environment(tmp_path)
    code = '''
import json, os, resource, sys
sys.path.insert(0, sys.argv[1])
from demiflow.forge.runs.worker import allocation_guard
allocation_guard(dict(memory_bytes=64 * 1024**2, max_rss_bytes=1024**3))
assert os.environ['MALLOC_ARENA_MAX'] == '2'
assert sys.flags.isolated == 1
soft, hard = resource.getrlimit(resource.RLIMIT_AS)
assert soft == hard and soft > 0
assert resource.getrlimit(resource.RLIMIT_NOFILE)[0] <= 1024
try:
    bytearray(512 * 1024**2)
except MemoryError:
    print(json.dumps(dict(address_limit=soft, oversized_allocation='rejected')))
else:
    raise AssertionError('allocation guard was bypassed')
'''
    result = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(package)],
                            env=worker, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['oversized_allocation'] == 'rejected'
