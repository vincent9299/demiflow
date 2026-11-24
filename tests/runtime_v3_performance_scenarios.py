"""Owned V034 performance experiments; no production routing or fake success.

This file deliberately imports only the standard library at module import time.
The same workload code is run in isolated processes against a pinned historical
checkout and candidate checkout. Timed work always enters the public Dataset API.
Process/disk samples are bounded observations, not an RSS/physical-copy guarantee.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import dataclasses
from collections.abc import Mapping
import fcntl
import importlib.abc
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
import traceback

OWN_TREE = Path(__file__).resolve().parents[1]
PLAN_PATH = OWN_TREE / 'docs/runtime-v3-review-20261115/PERFORMANCE-PLAN-20261116.json'
MODULUS = 1 << 256
TIMING_BOUNDARY = {
    'schema': 'demiflow.performance.timing.v1',
    'clock': 'time.monotonic_ns',
    'start': 'immediately before RunContext.start',
    'stop': 'after public result consumption, original ACK and run/executor close',
    'included': ['declaration/admission', 'business worker startup/import/code-pin',
                 'public result consumption/oracle', 'original ACK', 'run/executor cleanup'],
    'excluded': ['child process spawn/initial driver imports', 'input digest',
                 'HTTP/Ray observer setup/teardown', 'filesystem probes', 'source snapshots'],
}
_STORAGE_ROLES = ('source_checkout', 'source_package', 'harness', 'trial_root',
                  'run_directory', 'capacity', 'resource_home', 'forge_domain', 'location_cache',
                  'temporary', 'tmp')
_MOUNT_IDENTITY = ('device', 'fsid', 'fs_type', 'mount_source', 'mount_root',
                   'mount_point', 'mount_options', 'super_options')


def filesystem_identity(path):
    """Observe one path's actual Linux mount, including not-yet-created children.

    No directory walk or environment dump: only bounded mount metadata and a
    stat of the closest existing ancestor. A later probe detects child mounts.
    Missing evidence is explicit; matching path spelling never proves storage.
    """
    result = {'path': str(Path(path).absolute()), 'status': 'unknown'}
    try:
        resolved = Path(path).resolve()
        existing = resolved
        for _ in range(256):
            if existing.exists():
                break
            existing = existing.parent
        else:
            raise ValueError('filesystem ancestor bound exceeded')
        device = existing.stat().st_dev
        with open('/proc/self/mountinfo', 'rb') as stream:
            raw = stream.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('mount evidence byte bound exceeded')
        unescape = lambda value: re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), value)
        candidates = []
        for line in raw.decode().splitlines():
            fields = line.split(); separator = fields.index('-')
            mount = Path(unescape(fields[4]))
            if (fields[2] == f'{os.major(device)}:{os.minor(device)}'
                    and resolved.is_relative_to(mount)):
                candidates.append((len(mount.parts), fields, separator, line))
        if not candidates:
            raise ValueError('actual path device has no matching mount evidence')
        _, fields, separator, line = max(candidates, key=lambda item: item[0])
        result.update(status='observed', resolved_path=str(resolved),
            observed_existing_path=str(existing), path_exists=existing == resolved,
            device=fields[2], fsid=os.statvfs(existing).f_fsid,
            fs_type=fields[separator + 1], mount_source=unescape(fields[separator + 2]),
            mount_root=unescape(fields[3]), mount_point=unescape(fields[4]),
            mount_options=sorted(fields[5].split(',')),
            super_options=sorted(fields[separator + 3].split(',')),
            mount_id=fields[0], mount_record_sha256=hashlib.sha256(line.encode()).hexdigest())
    except (OSError, ValueError, IndexError, AttributeError) as exc:
        result['error'] = type(exc).__name__ + ': ' + str(exc)[:256]
    return result


def _storage_locations(source_root, output_root):
    source, output = Path(source_root).resolve(), Path(output_root).resolve()
    return dict(source_checkout=str(source), source_package=str(source / 'demiflow' / '__init__.py'),
        harness=str(Path(__file__).resolve()), trial_root=str(output),
        run_directory=str(output / 'run'), capacity=str(output / 'capacity'),
        resource_home=str(output / 'resource-home'), forge_domain=str(output / 'forge-domain'),
        location_cache=str(output / 'locations'),
        temporary=str(output / 'temporary'), tmp=str(output / 'tmp'))


def _storage_probe(source_root, output_root):
    """Same narrow read-only observation in the driver or a selected Ray node."""
    report = {'status': 'unknown', 'locations': {}}
    try:
        import demiflow
        expected = Path(source_root).resolve() / 'demiflow' / '__init__.py'
        if Path(demiflow.__file__).resolve() != expected:
            raise ValueError('filesystem probe imported a different source tree')
        with open('/proc/sys/kernel/random/boot_id') as stream:
            boot = stream.read(128).strip()
        if not re.fullmatch(r'[0-9a-f-]{36}', boot):
            raise ValueError('host boot identity unavailable')
        report.update(hostname=os.uname().nodename, boot_id=boot,
                      mount_namespace=os.readlink('/proc/self/ns/mnt'))
        report['locations'] = {name: filesystem_identity(path)
                               for name, path in _storage_locations(source_root, output_root).items()}
        report['status'] = ('observed' if all(v['status'] == 'observed'
                                           for v in report['locations'].values()) else 'unknown')
    except (OSError, ValueError, ImportError) as exc:
        report['error'] = type(exc).__name__ + ': ' + str(exc)[:256]
    return report


def _ray_storage_probe(source_root, output_root):
    import ray
    return {**_storage_probe(source_root, output_root),
            'node_id': ray.get_runtime_context().get_node_id()}


def _capture_storage_environment(source_root, output_root, provider):
    result = {'driver': _storage_probe(source_root, output_root), 'nodes': [], 'errors': []}
    if provider == 'local':
        return result
    refs = []
    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        nodes = [node['NodeID'] for node in ray.nodes() if node['Alive']]
        if not 1 <= len(nodes) <= 8:
            raise ValueError('owned Ray storage probe requires one to eight actual nodes')
        probe = ray.remote(num_cpus=0, num_gpus=0, max_retries=0, max_calls=1)(_ray_storage_probe)
        refs = [probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)).remote(
            str(source_root), str(output_root)) for node in nodes]
        observed = ray.get(refs, timeout=30)
        if [item.get('node_id') for item in observed] != nodes:
            raise ValueError('storage probe did not run on every selected real node')
        result['nodes'] = observed
    except Exception as exc:
        result['errors'].append(type(exc).__name__ + ': ' + str(exc)[:256])
        for ref in refs:
            try:
                ray.cancel(ref, force=True)
            except Exception:
                pass  # Only this probe's own tasks; never cluster/service cleanup.
    return result


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _plain(value):
    if dataclasses.is_dataclass(value):
        return {field.name: _plain(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


def _git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()


def snapshot(root):
    """Bound source evidence, including dirty input bytes, without git mutation."""
    root = Path(root).resolve()
    files = sorted((root / 'demiflow').rglob('*.py'))
    if len(files) > 10000:
        raise ValueError('source evidence file budget exceeded')
    harness_files = [Path(__file__), Path(__file__).with_name('runtime_v3_performance_ray.py')]
    harness = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in harness_files}
    return {'root': str(root), 'head': _git(root, 'rev-parse', 'HEAD'),
            'dirty': _git(root, 'status', '--porcelain'),
            'production_clean': not _git(root, 'status', '--porcelain', '--untracked-files=all', '--', 'demiflow'),
            'production_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in files},
            'harness_sha256': hashlib.sha256(_json(harness).encode()).hexdigest(),
            'harness_inputs': harness,
            'plan_sha256': hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()}


def _digest(rows, *, ordered=False):
    # Order independent but multiplicity sensitive; original per-case oracles
    # below additionally validate keys, values, nulls and duplicate occurrence.
    count, total, xor = 0, 0, 0
    sequence = hashlib.sha256()
    for row in rows:
        encoded = _json(row).encode()
        value = int.from_bytes(hashlib.sha256(encoded).digest(), 'big')
        if ordered: sequence.update(len(encoded).to_bytes(8, 'big') + encoded)
        total = (total + value) % MODULUS
        xor ^= value
        count += 1
    result = {'rows': count, 'sum256': f'{total:064x}', 'xor256': f'{xor:064x}'}
    if ordered: result['sequence_sha256'] = sequence.hexdigest()
    return result


def _input_rows(case, side='left'):
    if case == 'small_data':
        yield from ({'id': i} for i in range(31))
    elif case in ('slow_api', 'uneven_model'):
        yield from ({'id': i, 'text': str(i)} for i in range(64))
    elif case == 'cpu_gpu':
        yield from ({'id': i} for i in range(32))
    elif case == 'expand_100x100':
        yield from ({'id': i} for i in range(100))
    elif case == 'hot_key':
        for i in range(16384):
            yield {'k': 0 if i % 10 else 1 + (i // 10) % 127, 'v': i, 'payload': 'x' * 4096}
    elif case == 'large_graph':
        yield from ({'id': i} for i in range(64))
    elif case == 'finite_sealed':
        for i in range(40000):
            if side == 'left':
                yield {'k': i, 'left_payload': bytes([i % 251]) * 8192}
            else:
                yield {'k': i, 'right_payload': bytes([i % 251]) * 8192, 'tag': 'base'}
        yield ({'k': None, 'left_payload': b'null key'} if side == 'left' else
               {'k': 0, 'right_payload': b'duplicate', 'tag': 'duplicate'})
    elif case == 'fast_cpu':
        yield from ({'id': i, 'v': i, 'text': 'x' * 32} for i in range(20000))
    else:
        raise ValueError('unknown workload ' + case)


def input_digest(case):
    # Stream the *actual* deterministic inputs. Binary wide payloads remain
    # binary; no sample-derived width or materialized 650 MiB driver fixture.
    digest, count = hashlib.sha256(), 0
    for side in (('left', 'right') if case == 'finite_sealed' else ('left',)):
        digest.update(side.encode() + b'\0')
        for row in _input_rows(case, side):
            for key, value in sorted(row.items()):
                data = value if isinstance(value, bytes) else _json(value).encode()
                digest.update(key.encode() + b'\0' + len(data).to_bytes(8, 'big') + data)
            digest.update(b'\xff')
            count += 1
    return {'sha256': digest.hexdigest(), 'rows': count}


def expected_rows(case):
    if case == 'fast_cpu':
        yield from ({'id': i, 'v': i * 3 + 1} for i in range(20000) if i % 2)
    elif case == 'small_data':
        values = [a for i in range(31) for a in (i, i + 100) if a % 2 == 0]
        yield from ({'n': len(values[j:j+5]), 'v': sum(values[j:j+5])} for j in (0, 5, 10))
    elif case in ('slow_api', 'uneven_model'):
        yield from ({'id': i, 'text': str(i), 'embedding': [1., 0., 0.]} for i in range(64))
    elif case == 'cpu_gpu':
        yield from ({'id': i, 'answer': 4096 * 4095 + i * 4096} for i in range(32))
    elif case == 'expand_100x100':
        yield from ({'id': i, 'child': j, 'checksum': i * 100 + j}
                    for i in range(100) for j in range(100))
    elif case == 'hot_key':
        counts = {}
        for row in _input_rows(case):
            k = row['k']; old = counts.get(k, {'k': k, 'count': 0, 'sum': 0})
            old['count'] += 1; old['sum'] += row['v']; counts[k] = old
        yield from counts.values()
    elif case == 'large_graph':
        yield from ({'id': i, 'branch': b, 'value': i + 4 * (b + 1)}
                    for b in range(16) for i in range(64))
    elif case == 'finite_sealed':
        yield from ({'k': i, 'tag': 'base'} for i in range(40000))
        yield {'k': 0, 'tag': 'duplicate'}
        yield {'k': None, 'tag': 'unmatched'}


def _source(case, root, side='left', cancellation=False):
    def rows():
        root_path = Path(root)
        for ordinal, row in enumerate(_input_rows(case, side)):
            if ordinal == 0:
                (root_path / ('source-start-' + side)).write_text(str(time.time_ns()))
            yield row
            if ordinal == 0 and cancellation:
                until = time.monotonic() + 15
                while not (root_path / 'cancel-requested').exists():
                    if time.monotonic() >= until:
                        raise RuntimeError('owned cancellation controller did not respond')
                    time.sleep(.005)
            elif ordinal == 0 and case in ('slow_api', 'uneven_model'):
                # A true early-result handshake: the source cannot reach EOI
                # before the public consumer observes the first native result.
                until = time.monotonic() + 30
                while not (root_path / 'first-public-result').exists():
                    if time.monotonic() >= until:
                        raise RuntimeError('native first result failed to precede source EOI')
                    time.sleep(.005)
        (root_path / ('source-eoi-' + side)).write_text(str(time.time_ns()))
    return rows


def _cancel_gate(row, *, root):
    marker = Path(root) / 'source-start-left'
    if not marker.exists(): marker.write_text(str(time.time_ns()))
    until = time.monotonic() + 15
    while not (Path(root) / 'cancel-requested').exists():
        if time.monotonic() >= until: raise RuntimeError('cancel controller did not respond')
        time.sleep(.005)
    return row


def _wide_check(row):
    if row['k'] is None:
        assert row == {'k': None, 'left_payload': b'null key'}
        return {'k': None, 'tag': 'unmatched'}
    expected = bytes([row['k'] % 251]) * 8192
    assert row['left_payload'] == expected
    assert row['right_payload'] == (b'duplicate' if row['tag'] == 'duplicate' else expected)
    return {'k': row['k'], 'tag': row['tag']}


def _reduce(acc, row):
    return {'k': row['k'], 'count': (acc or {}).get('count', 0) + 1,
            'sum': (acc or {}).get('sum', 0) + row['v']}


def _cpu(row):
    assert os.environ.get('CUDA_VISIBLE_DEVICES') == '', 'CPU stage inherited CUDA'
    return row


def _gpu(row, *, evidence):
    import torch
    from demiflow.resources.capacity import inherited_envelope
    grant = inherited_envelope()
    assert grant and grant['allocation_id']
    assert torch.cuda.is_available() and torch.cuda.device_count() == 1
    torch.cuda.reset_peak_memory_stats()
    values = torch.arange(4096, dtype=torch.float32, device='cuda')
    answer = int((values * 2 + row['id']).sum().item())
    torch.cuda.synchronize()
    _write(Path(evidence) / f'cuda-{os.getpid()}-{row["id"]}.json', {
        'allocation_id': grant['allocation_id'], 'pid': os.getpid(),
        'visible': os.environ['CUDA_VISIBLE_DEVICES'],
        'allocated_peak': torch.cuda.max_memory_allocated(),
        'reserved_peak': torch.cuda.max_memory_reserved()})
    return {'id': row['id'], 'answer': answer}


def build_dataset(api, spec, root, *, endpoint=None, cancellation=False):
    case = spec['id']
    src = (api.from_items(list(_input_rows(case))) if case == 'large_graph'
           else api.from_iter(_source(case, str(root), cancellation=cancellation)))
    if case == 'large_graph' and cancellation:
        src = src.map(_cancel_gate, fn_kwargs={'root': str(root)})
    if case == 'fast_cpu':
        return src.map(lambda r: {**r, 'v': r['v'] * 3 + 1}).filter(lambda r: r['v'] % 2 == 0).map(
            lambda r: {'id': r['id'], 'v': r['v']})
    if case == 'small_data':
        return (src.map(lambda r: {'a': r['id']}).flat_map(lambda r: [r, {'a': r['a'] + 100}])
                .filter(lambda r: r['a'] % 2 == 0)
                .map_batches(lambda b: [{'n': len(b), 'v': int(b['a'].sum())}],
                             batch_size=5, batch_format='pandas').limit(3))
    if case in ('slow_api', 'uneven_model'):
        from demiflow.embeddings import EmbeddingModel
        return src.map_embeddings(model=EmbeddingModel('fixture', 'v1', 3, endpoint + '/v1'),
            inputs={'text': 'text'}, batch_size=1, concurrency=4 if case == 'slow_api' else 8,
            max_requests=64, flush_interval=.04)
    if case == 'cpu_gpu':
        return src.map(_cpu).map(_gpu, fn_kwargs={'evidence': str(root)},
            backend_options={'ray': {'worker_resources': {'gpu': 1, 'memory_bytes': 2*1024**3}}}).map(_cpu)
    if case == 'expand_100x100':
        return src.flat_map(lambda r: [{'id': r['id'], 'child': j} for j in range(100)]).map(
            lambda r: {**r, 'checksum': r['id'] * 100 + r['child']})
    if case == 'hot_key':
        return src.reduce_by_key('k', _reduce)
    if case == 'large_graph':
        branches = []
        for branch in range(16):
            part = src.map(lambda r, b=branch: {'id': r['id'], 'branch': b, 'value': r['id'] + b + 1})
            for _ in range(3):
                part = part.map(lambda r: {**r, 'value': r['value'] + r['branch'] + 1})
            branches.append(part)
        return branches[0].union(*branches[1:])
    if case == 'finite_sealed':
        right = api.from_iter(_source(case, str(root), 'right', cancellation))
        return src.join(right, on='k', how='left').map(_wide_check)
    raise ValueError(case)


@contextmanager
def http_fixture(root, case, host='127.0.0.1'):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    calls, lock = [], threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get('Content-Length', 0))
            if not 0 < length <= 128 * 1024:
                self.send_error(413); return
            body = json.loads(self.rfile.read(length))
            values = body['input']
            assert isinstance(values, list) and len(values) == 1
            item = int(values[0]); delay = .03 if case == 'slow_api' else (.25 if item % 16 == 0 else .01)
            started = time.monotonic_ns()
            (Path(root) / 'http-started').write_text(str(started))
            time.sleep(delay)
            raw = _json({'data': [{'index': 0, 'embedding': [1., 0., 0.]}],
                         'usage': {'prompt_tokens': 1}}).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
            with lock:
                if len(calls) >= 256:
                    raise AssertionError('fixture request count exceeded finite budget')
                calls.append({'id': item, 'started_ns': started, 'finished_ns': time.monotonic_ns(),
                              'request_bytes': length, 'response_bytes': len(raw)})
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer((host, 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        yield f'http://{host}:{server.server_port}', calls
    finally:
        server.shutdown(); server.server_close(); thread.join(5)
        _write(Path(root) / 'http-calls.json', calls)


class ImportObserver(importlib.abc.MetaPathFinder):
    def __init__(self):
        self.attempts = set()
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ray.data' or fullname.startswith('ray.data.'):
            if len(self.attempts) < 1024:
                self.attempts.add(fullname)
        return None


class Observations:
    """Wrappers delegate original authorities, retaining bounded summaries only."""
    def __init__(self):
        self.calls, self.cuts, self.plans = [], [], []
        self.ray_observer = None
        self.copy = {'status': 'partial', 'driver_control_encode': {'calls': 0, 'bytes': 0},
                     'driver_control_decode': {'calls': 0, 'bytes': 0},
                     'unobserved': 'worker/Arrow/DataFusion/CUDA internal copies and Ray Core serialization outside this codec; bytes below are serialized boundary bytes, not memory-copy counts'}
        self.patches = []
    def patch(self, owner, name, fn):
        old = getattr(owner, name); self.patches.append((owner, name, old)); setattr(owner, name, fn)
    def __enter__(self):
        import importlib.util
        if importlib.util.find_spec('demiflow.execution.executors.dataflow_api') is None:
            self.copy = {'status': 'not_observable', 'reason': 'historical carrier has no matching v3 codec boundary'}
            return self
        # Import the consumer first, then patch both aliases, so the first API
        # call is observed rather than getting an unpatched late import alias.
        from demiflow.execution.executors import dataflow_api
        from demiflow.execution.action_driver import dataflow
        from demiflow.execution.hosts import endpoints as dataflow_hosts
        from demiflow.execution.contracts import dataflow_codec
        for name, counter in (('encode_wire', 'driver_control_encode'), ('decode_wire', 'driver_control_decode')):
            original_codec = getattr(dataflow_codec, name)
            def codec(value, *, type_name, bounds, _original=original_codec, _name=name, _counter=counter):
                result = _original(value, type_name=type_name, bounds=bounds)
                self.copy[_counter]['calls'] += 1
                self.copy[_counter]['bytes'] += len(result if _name == 'encode_wire' else value)
                return result
            self.patch(dataflow_codec, name, codec)
            if getattr(dataflow_hosts, name, None) is original_codec:
                self.patch(dataflow_hosts, name, codec)
        original = dataflow.execute_action
        def execute(request, *, policy, bounds):
            call = {'kind': request.kind, 'provider': policy.provider, 'observed_at_ns': time.monotonic_ns()}
            self.calls.append(call)
            try: return original(request, policy=policy, bounds=bounds)
            finally: call['prepare_and_start_s'] = (time.monotonic_ns()-call['observed_at_ns'])/1e9
        self.patch(dataflow, 'execute_action', execute)
        if getattr(dataflow_api, 'execute_action', None) is original:
            self.patch(dataflow_api, 'execute_action', execute)
        start = dataflow.DataflowExecution.start
        def started(execution):
            result = start(execution)
            if len(self.plans) < 8:
                self.plans.append({'dag_id': execution._graph.dag_id,
                    'plan_id': execution._physical.plan_id,
                    'nodes': len(execution._graph.nodes), 'bounds': _plain(execution._bounds),
                    'policy': _plain(execution._policy)})
            return result
        self.patch(dataflow.DataflowExecution, 'start', started)
        if self.ray_observer is not None:
            activate = dataflow.DataflowExecution._activate
            def activated(execution, lease, stage):
                result = activate(execution, lease, stage)
                self.ray_observer.register(execution, lease)
                return result
            self.patch(dataflow.DataflowExecution, '_activate', activated)
        advance = dataflow.DataflowExecution._advance_cut
        def advanced(execution):
            result = advance(execution)
            image = execution._restore_image
            if image and (not self.cuts or self.cuts[-1]['commit_id'] != image.commit_id):
                if len(self.cuts) >= 16384:
                    raise AssertionError('performance observer cut budget exceeded')
                entry = execution._action.context.journal.execution_commit(image.commit_id)
                self.cuts.append({'commit_id': image.commit_id, 'final': image.cut.final,
                                  'observed_at_ns': time.monotonic_ns(),
                                  'status': entry['status']})
            return result
        self.patch(dataflow.DataflowExecution, '_advance_cut', advanced)
        return self
    def __exit__(self, *args):
        for owner, name, old in reversed(self.patches):
            setattr(owner, name, old)


class Sampler:
    """Only own descendants and owned output directory; every scan is bounded."""
    def __init__(self, root, pid=None):
        self.root, self.pid = Path(root), pid or os.getpid()
        self.stop = threading.Event(); self.thread = None
        self.samples = 0; self.errors = []; self.known = set()
        self.peaks = {'rss_bytes': 0, 'fd_count': 0, 'disk_bytes': 0, 'disk_files': 0,
                      'processes': 0, 'pool_charged': {}, 'pool_active': 0}
        self.last_pool = None; self.scan_ns = 0
    def _tree(self):
        queue, found = [self.pid], set()
        while queue:
            pid = queue.pop()
            if pid in found: continue
            if len(found) >= 512: raise ValueError('owned process-tree sampling cap exceeded')
            base = Path('/proc') / str(pid)
            try:
                stat = (base / 'stat').read_text().rsplit(')', 1)[1].split()
                self.known.add((pid, stat[19]))
                found.add(pid)
                tasks = list((base / 'task').iterdir())
                if len(tasks) > 4096: raise ValueError('thread sampling cap exceeded')
                for task in tasks:
                    try: queue.extend(map(int, (task / 'children').read_text().split()))
                    except FileNotFoundError: pass
            except FileNotFoundError: pass
        rss, fds = 0, 0
        for pid in found:
            try:
                base = Path('/proc') / str(pid)
                for line in (base / 'status').read_text().splitlines():
                    if line.startswith('VmRSS:'): rss += int(line.split()[1]) * 1024
                with os.scandir(base / 'fd') as entries:
                    for number, _ in enumerate(entries, 1):
                        if number > 65536: raise ValueError('FD sampling cap exceeded')
                        fds += 1
            except FileNotFoundError: pass
        return rss, fds, len(found)
    def _disk(self):
        count, total, stack = 0, 0, [self.root]
        while stack:
            directory = stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    count += 1
                    if count > 131072: raise ValueError('owned directory sampling cap exceeded')
                    try:
                        if entry.is_symlink(): continue
                        if entry.is_dir(follow_symlinks=False): stack.append(Path(entry.path))
                        else: total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError: pass
        return total, count
    def _pool(self):
        # Read the existing authority under its existing lock. The benchmark
        # never instantiates an allocator or creates a reservation/lock file.
        pool = self.root / 'capacity' / 'capacity'
        lock = pool / 'capacity.lock'
        if not lock.exists(): return None
        descriptor = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
            active, charged = [], {}
            with os.scandir(pool / 'allocations') as entries:
                for count, entry in enumerate(entries):
                    if count >= 16384: raise ValueError('pool observation file cap exceeded')
                    if not entry.name.endswith('.json'): continue
                    with open(entry.path, 'rb') as stream:
                        raw = stream.read(8193)
                    if len(raw) > 8192: raise ValueError('pool observation byte cap exceeded')
                    record = json.loads(raw)
                    if record['state'] == 'RELEASED': continue
                    active.append(record)
                    for key, value in record['effective'].items():
                        charged[key] = charged.get(key, 0) + value
            return {'active': active, 'charged': charged, 'authority_path': str(pool)}
        finally:
            os.close(descriptor)
    def sample(self, slow=True):
        start = time.monotonic_ns()
        try:
            rss, fd, processes = self._tree()
            for key, val in [('rss_bytes', rss), ('fd_count', fd), ('processes', processes)]:
                self.peaks[key] = max(self.peaks[key], val)
            if slow:
                size, count = self._disk()
                self.peaks['disk_bytes'] = max(size, self.peaks['disk_bytes'])
                self.peaks['disk_files'] = max(count, self.peaks['disk_files'])
                self.last_pool = self._pool()
                if self.last_pool is not None:
                    self.peaks['pool_active'] = max(len(self.last_pool['active']), self.peaks['pool_active'])
                    for key, value in self.last_pool['charged'].items():
                        self.peaks['pool_charged'][key] = max(value, self.peaks['pool_charged'].get(key, 0))
            self.samples += 1
        except Exception as exc:
            if len(self.errors) < 20: self.errors.append(repr(exc))
        self.scan_ns += time.monotonic_ns() - start
    def __enter__(self):
        def loop():
            tick = 0
            while not self.stop.is_set():
                self.sample(tick % 10 == 0); tick += 1
                self.stop.wait(.05)
        self.thread = threading.Thread(target=loop, daemon=True); self.thread.start(); return self
    def __exit__(self, *args):
        self.stop.set(); self.thread.join(10); self.sample()
    def result(self):
        alive = []
        for pid, start in sorted(self.known):
            if pid == self.pid: continue
            try:
                fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                if fields[19] == start and fields[0] != 'Z': alive.append(pid)
            except FileNotFoundError: pass
        return {'sampled_peaks': self.peaks, 'samples': self.samples, 'errors': self.errors,
                'sampling_wall_s': self.scan_ns / 1e9, 'last_original_pool': self.last_pool,
                'remaining_observed_descendants_before_driver_exit': alive,
                'observed_process_births': sorted(self.known),
                'scope': 'owned child process tree and owned trial directory only; sampled, not hard RSS'}


def run_child(config):
    root = Path(config['output_root']); root.mkdir(parents=True, exist_ok=True)
    (root / 'temporary').mkdir(exist_ok=True)
    spec, variant, provider = config['spec'], config['variant'], config['provider']
    expected_root = Path(variant['root']).resolve()
    private_environment = {
        'DEMIFLOW_RESOURCE_ROOT': str(root / 'capacity'),
        'DEMIFLOW_RESOURCE_HOME': str(root / 'resource-home'),
        'DEMIFLOW_FORGE_DOMAIN': str(root / 'forge-domain'),
        'DEMIFLOW_LOCATION_CACHE': str(root / 'locations'),
    }
    for name, value in private_environment.items():
        assert os.environ.get(name) == value, f'{name} must belong to this trial'
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(expected_root)
    from demiflow import data
    from demiflow.execution.pipeline.run_context import RunContext
    cancellation = config['phase'] == 'cancel'
    facts = {'schema': 'demiflow.performance.trial.v1', **config,
             'private_environment': private_environment,
             'input': input_digest(spec['id']), 'status': 'failed'}
    imports, observations = ImportObserver(), Observations()
    sys.meta_path.insert(0, imports)
    first, started, action_done, closed = None, None, None, None
    cancel_requested, cancel_thread = [], None
    calls, output, session, ray_observer = [], None, None, None
    error = None
    profiler = None
    if config.get('profile'):
        import cProfile
        profiler = cProfile.Profile()
    source_eoi_at_first = None
    public_rows_observed = 0
    try:
        with ExitStack() as outer:
            endpoint = None
            if spec['id'] in ('slow_api', 'uneven_model'):
                endpoint, calls = outer.enter_context(http_fixture(root, spec['id'], config.get('http_host', '127.0.0.1')))
            if provider != 'local':
                import ray
                address = config.get('ray_address')
                if not address:
                    raise RuntimeError('owned Ray address required; never connect to an implicit cluster')
                ray.init(address=address, log_to_driver=False, runtime_env={'env_vars': {
                    'PYTHONPATH': str(expected_root) + ':' + str(OWN_TREE / 'tests'),
                    **private_environment,
                    'PYTHONDONTWRITEBYTECODE': '1'}})
                outer.callback(ray.shutdown)
                nodes = [n for n in ray.nodes() if n['Alive']]
                facts['ray_nodes'] = [{'node_id': n['NodeID'], 'hostname': n['NodeManagerHostname']} for n in nodes]
                if provider == 'two_hosts' and len({n['NodeManagerHostname'] for n in nodes}) < 2:
                    raise RuntimeError('two_hosts requires two actual physical hostnames')
                from runtime_v3_performance_ray import RayObservations
                ray_observer = outer.enter_context(RayObservations(expected_root, root / 'capacity',
                    require_two_hosts=provider == 'two_hosts'))
                observations.ray_observer = ray_observer
            outer.enter_context(observations)
            facts['comparison_environment'] = {
                'schema': 'demiflow.performance.storage.v1', 'timing': TIMING_BOUNDARY,
                'before': _capture_storage_environment(expected_root, root, provider)}
            # Registered before business_closed: LIFO closes the timer first,
            # then probes while this trial's original Ray connection is alive.
            def storage_after():
                facts['comparison_environment']['after'] = _capture_storage_environment(expected_root, root, provider)
            outer.callback(storage_after)
            def business_closed():
                nonlocal closed
                if closed is None: closed = time.monotonic_ns()
            outer.callback(business_closed)
            if profiler: profiler.enable()
            started = time.monotonic_ns()
            with RunContext.start(root / 'run', limits=spec['limits']) as run:
                facts['run_directory'] = str(run.directory)
                if cancellation:
                    def cancel():
                        until = time.monotonic() + spec['limits']['timeout_s']
                        def entered():
                            if spec['id'] == 'cpu_gpu': return any(root.glob('cuda-*.json'))
                            if spec['id'] in ('slow_api', 'uneven_model'): return (root/'http-started').exists()
                            return (root / 'source-start-left').exists()
                        while not entered():
                            if time.monotonic() >= until: return
                            time.sleep(.005)
                        cancel_requested.append(time.monotonic_ns())
                        RunContext.cancel(run.ref)
                        (root / 'cancel-requested').write_text(str(cancel_requested[-1]))
                    cancel_thread = threading.Thread(target=cancel, daemon=True); cancel_thread.start()
                with ExitStack() as execution:
                    if provider == 'local':
                        native = dict(spec.get('native_resources', {})) or None
                        if native: native['partitions'] = config.get('partitions', 1)
                        session = execution.enter_context(data.local_execution(workers=spec['workers'],
                            worker_mode=spec.get('worker_mode', 'process'),
                            batch_rows=spec.get('batch_rows', 8192), memory_bytes=spec['limits']['memory_bytes'],
                            temp_directory=str(root / 'temporary'), native_resources=native))
                        api = data
                    else:
                        import inspect
                        from demiflow.data.api import DataAPI
                        from demiflow.execution.executors.ray import RayDatasetExecutor
                        parameters = inspect.signature(RayDatasetExecutor).parameters
                        options = {'resource_root': root}
                        if 'workers' not in parameters:
                            raise NotImplementedError('Ray public executor cannot yet declare the same bounded worker count')
                        options['workers'] = spec['workers']
                        if spec.get('native_resources'):
                            if 'native_resources' not in parameters:
                                raise NotImplementedError('Ray public executor cannot yet declare the same native envelope')
                            native = dict(spec['native_resources']); native['partitions'] = config.get('partitions', 1)
                            options['native_resources'] = native
                        executor = RayDatasetExecutor(**options)
                        execution.callback(executor.close); api = DataAPI(executor)
                    dataset = build_dataset(api, spec, root, endpoint=endpoint, cancellation=cancellation)
                    rows = (dataset.take_all() if spec['id'] == 'small_data' and not cancellation
                            else dataset.iter_rows())
                    if hasattr(rows, 'close'): execution.callback(rows.close)
                    def observed_rows():
                        nonlocal first, source_eoi_at_first, public_rows_observed
                        for row in rows:
                            public_rows_observed += 1
                            if first is None:
                                first = time.monotonic_ns()
                                source_eoi_at_first = (root / 'source-eoi-left').exists()
                                (root / 'first-public-result').write_text(str(first))
                            yield row
                    ordered = spec['id'] == 'small_data'
                    output = _digest(observed_rows(), ordered=ordered)
                    action_done = time.monotonic_ns()
                    if cancellation:
                        raise AssertionError('cancel trial completed normally; cancellation was not exercised')
                    oracle = _digest(expected_rows(spec['id']), ordered=ordered)
                    assert output == oracle, (output, oracle)
                    if session: facts['local_stats'] = session.stats
                    facts['run_limits'] = dict(run.limits)
            closed = time.monotonic_ns()
    except Exception as exc:
        if closed is None: closed = time.monotonic_ns()
        error = exc
        facts['error'] = {'type': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()}
    finally:
        if profiler:
            profiler.disable(); profiler.dump_stats(str(root / 'driver.prof'))
        if cancel_thread: cancel_thread.join(1)
        sys.meta_path.remove(imports)
    facts.update(output=output, elapsed_s=(closed-started)/1e9 if started else None,
        action_elapsed_s=(action_done-started)/1e9 if action_done else None,
        first_result_s=(first-started)/1e9 if first else None,
        throughput_rows_s=(output['rows'] * 1e9/(closed-started)) if output and closed else None,
        source_eoi_at_first_result=source_eoi_at_first,
        cancel_latency_s=(closed-cancel_requested[0])/1e9 if cancel_requested else None,
        child_execute_action_calls=observations.calls, cuts=observations.cuts, plans=observations.plans,
        copy_observation=observations.copy,
        ray_resources=ray_observer.result if ray_observer else None,
        public_rows_observed=public_rows_observed,
        ray_data_import_attempts=sorted(imports.attempts),
        http_request_count=len(calls), resources=None)
    facts['measurement_started_ns'] = started
    facts['measurement_closed_ns'] = closed
    loaded = {name: str(Path(module.__file__).resolve()) for name, module in tuple(sys.modules.items())
              if name.startswith('demiflow') and getattr(module, '__file__', None)}
    facts['loaded_modules'] = loaded
    assert all(Path(path).is_relative_to(expected_root) for path in loaded.values())
    is_v3 = bool(observations.calls)
    facts['carrier'] = ('unified_dataflow_v3' if is_v3 else
                        'historical_ray_data' if imports.attempts else 'historical_local')
    failures = []
    if variant['kind'] == 'candidate' and not is_v3: failures.append('public action did not enter execute_action')
    if is_v3 and imports.attempts: failures.append('unified candidate imported Ray Data')
    if error is None and is_v3 and (not observations.cuts or not observations.cuts[-1]['final']
                                   or observations.cuts[-1]['status'] != 'acked'):
        failures.append('successful action lacks original final ACK')
    if facts['resources']:
        if facts['resources']['errors']: failures.append('resource samples incomplete')
        pool = facts['resources']['last_original_pool']
        if pool and (pool['active'] or any(pool['charged'].values())): failures.append('original pool remains charged')
    if cancellation:
        text = '' if error is None else repr(error)
        if cancel_requested and ('CANCELLED' in text or 'cancel' in text.lower()) and not isinstance(error, AssertionError):
            facts['status'] = 'passed' if not failures else 'failed'
        else: failures.append('no actual cancellation exception after source entry')
    elif error is None:
        if spec['id'] in ('slow_api', 'uneven_model'):
            if len(calls) != 64 or source_eoi_at_first is not False:
                failures.append('native request count/early-result proof differs')
        if spec['id'] == 'large_graph' and is_v3 and observations.plans[0]['nodes'] < 65:
            failures.append('large graph did not retain declared logical occurrences')
        facts['status'] = 'passed' if not failures else 'failed'
    elif variant['kind'] in ('historical', 'reference') and isinstance(error, NotImplementedError):
        facts['status'] = 'unsupported'
        facts['carrier'] = 'not_executed'
    if provider != 'local':
        from runtime_v3_performance_ray import ray_receipt_errors
        remote_errors = ray_receipt_errors(facts['ray_resources'], require_two_hosts=provider == 'two_hosts')
        facts['limitations'] = ['Shared Ray service/object-store RSS is unattributed; business RSS samples can count shared pages repeatedly',
            'Disk samples cover the owned trial directory, excluding node-local worker artifact caches outside it']
        if remote_errors and facts['status'] == 'passed':
            failures.extend(remote_errors); facts['status'] = 'failed'
    facts['failures'] = failures
    _write(root / 'trial.json', facts)
    return facts


def measure_trial(spec, variant, phase, repetition, provider, output_root, **settings):
    """One child, one immutable input/budget, no global process/service cleanup."""
    output_root = Path(output_root).resolve()
    if not str(output_root).startswith('/tmp/demiflow-') and not str(output_root).startswith(
            '/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/'):
        raise ValueError('benchmark output must be an explicitly owned test root')
    output_root.mkdir(parents=True, exist_ok=False)
    variant = dict(variant, root=str(Path(variant['root']).resolve()))
    before = snapshot(variant['root'])
    config = dict(spec=spec, variant=variant, phase=phase, repetition=repetition,
                  provider=provider, output_root=str(output_root), **settings)
    _write(output_root / 'config.json', config)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1',
               PYTHONPATH=variant['root'] + ':' + str(OWN_TREE / 'tests'),
               DEMIFLOW_RESOURCE_ROOT=str(output_root / 'capacity'),
               DEMIFLOW_RESOURCE_HOME=str(output_root / 'resource-home'),
               DEMIFLOW_FORGE_DOMAIN=str(output_root / 'forge-domain'),
               DEMIFLOW_LOCATION_CACHE=str(output_root / 'locations'),
               TMPDIR=str(output_root / 'tmp'))
    Path(env['TMPDIR']).mkdir()
    selected_gpu = None
    if spec['id'] == 'cpu_gpu':
        raw = subprocess.check_output(['nvidia-smi', '--query-gpu=index,uuid,memory.total,memory.used,utilization.gpu',
            '--format=csv,noheader,nounits'], text=True, timeout=15)
        for line in raw.splitlines():
            index, uuid, total, used, busy = [value.strip() for value in line.split(',')]
            if index == '0':
                if int(used) or int(busy): raise RuntimeError('authorized GPU0 is not idle')
                selected_gpu = {'device_id': uuid, 'type': 'gpu', 'memory_bytes': int(total)*1024**2, 'max_residents': 1}
        if selected_gpu is None: raise RuntimeError('physical GPU0 unavailable')
        path = output_root / 'actual-device-inventory.json'
        _write(path, {'devices': [selected_gpu]})
        env.update(DEMIFLOW_DEVICE_INVENTORY=str(path), CUDA_VISIBLE_DEVICES=selected_gpu['device_id'])
        (output_root / 'hardware-before.csv').write_text(raw)
    command = [variant.get('python', sys.executable), '-B', '-m', 'runtime_v3_performance_scenarios',
               '--child', str(output_root / 'config.json')]
    launched = time.monotonic()
    with (output_root / 'stdout.log').open('wb') as log:
        child = subprocess.Popen(command, cwd=variant['root'], env=env, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        with Sampler(output_root, child.pid) as sampler:
            try:
                returncode = child.wait(timeout=spec['limits']['timeout_s'] + 45)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGTERM)
                try: child.wait(timeout=10)
                except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); child.wait()
                returncode = child.returncode
    after = snapshot(variant['root'])
    report_path = output_root / 'trial.json'
    result = json.loads(report_path.read_text()) if report_path.exists() else {
        'status': 'failed', 'error': {'message': 'child exited without a trial receipt'}}
    result['resources'] = sampler.result()
    if result.get('ray_resources'):
        nodes = result['ray_resources'].get('nodes', [])
        result['resources']['driver_plus_worker_peak_sum'] = {
            key: sampler.peaks[key] + sum(node['sampled_peaks'][key] for node in nodes)
            for key in ('rss_bytes', 'fd_count', 'processes')}
        result['resources']['peak_sum_scope'] = 'sum of separately sampled driver and per-node business peaks; not a simultaneous RSS hard limit; shared Ray daemons/object store excluded'
    pool = sampler.last_pool
    if sampler.errors or pool and (pool['active'] or any(pool['charged'].values())):
        result['status'] = 'failed'
        result.setdefault('failures', []).append('resource observations incomplete or original capacity unreleased')
    births = result.get('resources', {}).get('observed_process_births', []) if result.get('resources') else []
    alive = []
    until = time.monotonic() + 3
    while True:
        alive = []
        for pid, birth in births:
            try:
                fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                if fields[19] == birth and fields[0] != 'Z': alive.append(pid)
            except FileNotFoundError: pass
        if not alive or time.monotonic() >= until: break
        time.sleep(.05)
    if alive:
        result['status'] = 'failed'
        result.setdefault('failures', []).append('owned process remains alive after driver exit')
    result['remaining_observed_descendants_after_driver_exit'] = alive
    if selected_gpu is not None:
        calls = [json.loads(path.read_text()) for path in sorted(output_root.glob('cuda-*.json'))]
        allocations = [json.loads(path.read_text()) for path in (output_root/'capacity'/'devices'/'allocations').glob('*.json')]
        deadline = time.monotonic() + 5
        while True:
            apps = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,gpu_uuid,used_gpu_memory',
                '--format=csv,noheader,nounits'], text=True, timeout=15)
            if selected_gpu['device_id'] not in apps or time.monotonic() >= deadline: break
            time.sleep(.05)
        result['gpu_evidence'] = {'device': selected_gpu, 'calls': calls, 'original_allocations': allocations,
                                  'compute_apps_after': apps}
        invalid = (selected_gpu['device_id'] in apps or any(a['state'] != 'RELEASED' for a in allocations))
        if result['status'] == 'passed' and phase != 'cancel':
            invalid = invalid or len(calls) != 32 or not allocations or any(
                call['visible'] != selected_gpu['device_id'] or not call['allocation_id'] for call in calls)
        if invalid:
            result['status'] = 'failed'; result.setdefault('failures', []).append('physical GPU grant/use/cleanup differs')
    result.update(returncode=returncode, subprocess_elapsed_s=time.monotonic()-launched,
                  command=command, source_before=before, source_after=after, source_unchanged=after == before)
    if returncode or after != before: result['status'] = 'failed'
    _write(output_root / 'trial.json', result)
    return result


def _percentile(values, percentile):
    return sorted(values)[max(0, math.ceil(len(values) * percentile) - 1)]


def required_cells():
    """The approved scope: nine Local classes and three Ray/two-host classes."""
    plan = json.loads(PLAN_PATH.read_text())
    cells = []
    for spec in plan['cases']:
        for provider in ('local', 'ray', 'two_hosts'):
            if provider != 'local' and spec['id'] not in plan['environment_plan'][provider]: continue
            suffixes = (':p1', ':p2') if spec['id'] == 'finite_sealed' else ('',)
            cells.extend(spec['id'] + ':' + provider + suffix for suffix in suffixes)
    return cells


def expected_baseline(cell):
    """The coordinator-approved reference is explicit per capability/carrier."""
    case, provider, *_ = cell.split(':')
    policy = json.loads(PLAN_PATH.read_text())['baseline_policy']
    if provider == 'local':
        entry = policy['local']
        return entry['reference' if case in entry['new_capability_cases'] else 'default']
    return policy[provider]


def _receipt_errors(row):
    errors = []
    if row.get('status') != 'passed': return errors
    before, after = row.get('source_before'), row.get('source_after')
    if not before or before != after or row.get('source_unchanged') is not True:
        errors.append('source/evidence changed within trial')
    if row.get('arm') == 'baseline':
        try: expected = expected_baseline(row['cell'])
        except (KeyError, ValueError): expected = None
        if (row.get('baseline_role') != expected or not before
            or expected is None or before.get('head') != expected['commit']
            or before.get('production_clean') is not True
            or row.get('variant', {}).get('kind') != expected['kind']):
            errors.append('baseline identity/capability role differs from approved environment policy')
    if row.get('returncode') != 0 or row.get('failures'):
        errors.append('exit status or recorded failures disagree')
    resources = row.get('resources')
    if not resources or resources.get('samples', 0) < 1 or resources.get('errors'):
        errors.append('resource samples missing/incomplete')
    elif any(resources.get('sampled_peaks', {}).get(k, -1) < 0
             for k in ('rss_bytes', 'fd_count', 'disk_bytes')):
        errors.append('resource metric missing/invalid')
    if resources:
        pool = resources.get('last_original_pool')
        if pool and (pool.get('active') or any(pool.get('charged', {}).values())):
            errors.append('original pool remains charged')
    if row.get('remaining_observed_descendants_after_driver_exit') != []:
        errors.append('owned physical process exit not proved')
    if row.get('provider') in ('ray', 'two_hosts'):
        from runtime_v3_performance_ray import ray_receipt_errors
        errors.extend(ray_receipt_errors(row.get('ray_resources'), require_two_hosts=row['provider'] == 'two_hosts'))
    elapsed = row.get('elapsed_s')
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
        errors.append('elapsed time invalid')
    else:
        for key in ('first_result_s', 'action_elapsed_s'):
            value = row.get(key)
            if value is not None and (type(value) not in (float, int) or
                    not math.isfinite(value) or not 0 <= value <= elapsed):
                errors.append(key + ' invalid')
        if row.get('phase') != 'cancel':
            if row.get('public_rows_observed') != row.get('output', {}).get('rows'):
                errors.append('public observed row count differs from the complete output')
            if row.get('first_result_s') is None or row.get('action_elapsed_s') is None:
                errors.append('successful nonempty workload lacks public timing')
            throughput = row.get('throughput_rows_s')
            expected = row.get('output', {}).get('rows', 0) / elapsed
            if type(throughput) not in (int, float) or not math.isfinite(throughput) or not math.isclose(throughput, expected, rel_tol=1e-6):
                errors.append('throughput does not use actual rows/full elapsed time')
    if row.get('arm') == 'candidate':
        if not row.get('child_execute_action_calls') or row.get('carrier') != 'unified_dataflow_v3':
            errors.append('candidate did not use original unified action')
        if row.get('ray_data_import_attempts'): errors.append('candidate imported Ray Data')
    if row.get('carrier') == 'unified_dataflow_v3' and row.get('phase') != 'cancel':
        cuts = row.get('cuts', [])
        if not cuts or cuts[-1].get('final') is not True or cuts[-1].get('status') != 'acked':
            errors.append('final original ACK absent')
    if row.get('phase') == 'cancel':
        latency = row.get('cancel_latency_s')
        if type(latency) not in (int, float) or not math.isfinite(latency) or latency < 0 or not row.get('error'):
            errors.append('actual cancellation receipt absent')
    return errors


def _comparison_environment_errors(rows):
    """Compare physical placement, not the different checkout/trial names.

    Functional receipts remain valid when storage is unobserved or different;
    no relative timing or net code gain is inferred from those trials.
    """
    errors, fingerprints = [], []
    for row in rows:
        label = f"{row.get('arm')}/{row.get('phase')}/{row.get('repetition')}"
        env = row.get('comparison_environment')
        try:
            if (not isinstance(env, dict) or env.get('schema') != 'demiflow.performance.storage.v1'
                    or env.get('timing') != TIMING_BOUNDARY or row.get('profile', False) is not False):
                raise ValueError('filesystem evidence/timing boundary missing or diagnostic profile enabled')
            start, stop = row.get('measurement_started_ns'), row.get('measurement_closed_ns')
            if (type(start) is not int or type(stop) is not int or not 0 < start < stop
                    or not math.isclose((stop - start) / 1e9, row['elapsed_s'], rel_tol=1e-9, abs_tol=1e-9)):
                raise ValueError('elapsed time is not bound to the declared measured interval')
            expected = _storage_locations(row['variant']['root'], row['output_root'])
            phases = []
            for phase in ('before', 'after'):
                value = env.get(phase)
                if not isinstance(value, dict) or value.get('errors') != []:
                    raise ValueError(phase + ' storage probe missing or incomplete')
                nodes = value.get('nodes')
                if not isinstance(nodes, list) or len(nodes) > 8:
                    raise ValueError('bounded real-node filesystem evidence missing')
                if row.get('provider') == 'local':
                    if nodes: raise ValueError('local trial claims unrelated remote nodes')
                else:
                    actual = {item['node_id'] for item in row.get('ray_nodes', [])}
                    observed = [item.get('node_id') for item in nodes]
                    if not actual or len(set(observed)) != len(observed) or set(observed) != actual:
                        raise ValueError('driver storage cannot stand in for actual Ray node storage')
                hosts = []
                for node in [value.get('driver'), *nodes]:
                    if (not isinstance(node, dict) or node.get('status') != 'observed'
                            or not node.get('hostname') or not node.get('mount_namespace')
                            or not re.fullmatch(r'[0-9a-f-]{36}', node.get('boot_id', ''))):
                        raise ValueError('host/boot/mount namespace evidence unavailable')
                    locations = node.get('locations')
                    if not isinstance(locations, dict) or set(locations) != set(_STORAGE_ROLES):
                        raise ValueError('required source/data/temporary filesystem location absent')
                    mounts = {}
                    for role, item in locations.items():
                        if (not isinstance(item, dict) or item.get('status') != 'observed'
                                or type(item.get('path_exists')) is not bool
                                or role in ('source_checkout', 'source_package', 'harness') and not item['path_exists']
                                or not re.fullmatch(r'\d+:\d+', item.get('device', ''))
                                or type(item.get('fsid')) is not int
                                or not re.fullmatch(r'[0-9a-f]{64}', item.get('mount_record_sha256', ''))
                                or any(not isinstance(item.get(k), str) or not item[k]
                                       for k in ('fs_type', 'mount_source', 'mount_root', 'mount_point',
                                                 'mount_id', 'resolved_path', 'observed_existing_path'))
                                or any(not isinstance(item.get(k), list) or not item[k]
                                       or any(type(option) is not str for option in item[k])
                                       for k in ('mount_options', 'super_options'))):
                            raise ValueError(role + ' actual filesystem identity incomplete')
                        path = Path(item['resolved_path'])
                        if (not path.is_absolute() or not path.is_relative_to(item['observed_existing_path'])
                                or not path.is_relative_to(item['mount_point'])
                                or role != 'harness' and str(path) != expected[role]):
                            raise ValueError(role + ' filesystem observation does not bind to the actual trial path')
                        mounts[role] = {key: item[key] for key in _MOUNT_IDENTITY}
                    hosts.append({'host': (node['hostname'], node['boot_id'], node['mount_namespace']),
                                  'locations': mounts})
                # Node IDs may change when an owned cluster is recreated. The
                # physical hosts, boots, namespaces and actual mounts must not.
                phases.append({'driver': hosts[0], 'nodes': sorted(hosts[1:], key=_json)})
            if phases[0] != phases[1]:
                raise ValueError('filesystem deployment changed during the timed trial')
            fingerprints.append(_json(phases[0]))
        except (ValueError, TypeError, KeyError) as exc:
            errors.append(label + ': ' + str(exc))
    if len(set(fingerprints)) > 1:
        errors.append('source/data/temporary physical deployment differs across arms or repetitions')
    return errors


def evaluate_report(report, *, required=None):
    """Fail closed on absent comparisons, invalid receipts or a >20% regression."""
    failures, missing, comparisons, incomparable = [], [], [], []
    declared = report.get('required_cells', [])
    required = list(required if required is not None else declared)
    if not required or len(set(required)) != len(required):
        return {'status': 'not_run', 'failures': [], 'missing': ['empty/duplicate required scope'], 'comparisons': []}
    known = set(required_cells())
    plan = {spec['id']: spec for spec in json.loads(PLAN_PATH.read_text())['cases']}
    if report.get('baseline_policy') != json.loads(PLAN_PATH.read_text())['baseline_policy']:
        failures.append({'reason': 'baseline environment policy missing or changed'})
    incomparable.extend(item for item in report.get('not_comparable_before_execution', [])
                        if item.get('cell') in required)
    oracles = {}
    if set(required) - known:
        failures.append({'reason': 'unknown environment/workload cells', 'cells': sorted(set(required)-known)})
    for cell in required:
        all_rows = [r for r in report.get('trials', []) if r.get('cell') == cell]
        rows = [r for r in all_rows if r.get('phase') == 'measure']
        arms = {arm: [r for r in rows if r.get('arm') == arm] for arm in ('baseline', 'candidate')}
        warmups = [r for r in all_rows if r.get('phase') == 'warmup']
        cancels = [r for r in all_rows if r.get('phase') == 'cancel' and r.get('arm') == 'candidate']
        if any(r.get('status') == 'failed' for r in all_rows):
            failures.append({'cell': cell, 'reason': 'real trial failed'})
        for row in all_rows:
            errors = _receipt_errors(row)
            if errors: failures.append({'cell': cell, 'reason': '; '.join(errors)})
        if (len(all_rows) != 13 or any(len(arms[a]) != 5 or {r.get('repetition') for r in arms[a]} != set(range(5)) for a in arms)
            or len(warmups) != 2 or {r.get('arm') for r in warmups} != set(arms)
            or any(r.get('repetition') != -1 for r in warmups) or len(cancels) != 1):
            missing.append({'cell': cell, 'reason': 'requires two independent warmups, five paired trials and real cancellation'})
            continue
        if any(r.get('status') != 'passed' for r in all_rows):
            missing.append({'cell': cell, 'reason': 'unsupported/not_run is not comparative success'})
            continue
        case = cell.split(':')[0]
        if case not in plan: continue
        if case not in oracles:
            oracles[case] = (input_digest(case), _digest(expected_rows(case), ordered=case == 'small_data'))
        expected_input, expected_output = oracles[case]
        if any(r['spec'] != plan[case] or r['input'] != expected_input or r['output'] != expected_output
               for r in rows + warmups):
            failures.append({'cell': cell, 'reason': 'workload differs from frozen plan/independent oracle'})
            continue
        if case == 'finite_sealed' and any(r.get('partitions') != int(cell.rsplit(':p', 1)[1]) for r in all_rows):
            failures.append({'cell': cell, 'reason': 'native partition declaration differs from cell'})
        fingerprints = {(r['input']['sha256'], _json(r['spec']), _json(r['output'])) for r in rows + warmups}
        if len(fingerprints) != 1:
            failures.append({'cell': cell, 'reason': 'input/output/budget comparison changed'}); continue
        for arm in arms:
            versions = {_json(r.get('source_before')) for r in all_rows if r.get('arm') == arm}
            if len(versions) != 1: failures.append({'cell': cell, 'reason': arm + ' version/evidence changed across trials'})
        evidence = {(r['source_before'].get('harness_sha256'), r['source_before'].get('plan_sha256')) for r in all_rows}
        if len(evidence) != 1 or any(not value for pair in evidence for value in pair):
            failures.append({'cell': cell, 'reason': 'harness/plan is not bound equally across arms'})
        if any(_receipt_errors(row) for row in rows): continue
        storage_errors = _comparison_environment_errors(all_rows)
        if storage_errors:
            incomparable.append({'cell': cell, 'reasons': storage_errors,
                'conclusion': 'not comparable; no relative performance or net code gain established'})
            continue
        summary = {'cell': cell}
        for arm, samples in arms.items():
            values = [r['elapsed_s'] for r in samples]
            summary[arm] = {'p50_s': _percentile(values, .5), 'p95_s': _percentile(values, .95),
                'samples_s': values, 'first_result_s': [r['first_result_s'] for r in samples],
                'throughput_rows_s': [r['throughput_rows_s'] for r in samples]}
        summary['p50_ratio'] = summary['candidate']['p50_s'] / summary['baseline']['p50_s']
        summary['p95_ratio'] = summary['candidate']['p95_s'] / summary['baseline']['p95_s']
        if max(summary['p50_ratio'], summary['p95_ratio']) > 1.2:
            failures.append({'cell': cell, 'reason': 'performance regression >20%; profile/fix or final review required',
                             'p50_ratio': summary['p50_ratio'], 'p95_ratio': summary['p95_ratio']})
        comparisons.append(summary)
    return {'status': 'failed' if failures else 'not_comparable' if incomparable else 'not_run' if missing else 'passed',
            'failures': failures, 'missing': missing, 'comparisons': comparisons,
            'not_comparable': incomparable}


def run_comparison(plan, baseline_roots, candidate_root, output_root, *, cells=None, **settings):
    """Formal comparisons require an explicitly frozen candidate revision."""
    if not settings.pop('frozen_candidate', False):
        raise ValueError('formal measurements require coordinator-frozen candidate')
    if settings.get('profile'):
        raise ValueError('diagnostic cProfile trials cannot be formal measurements')
    environment_settings = settings.pop('environment_settings', {})
    output_root = Path(output_root); output_root.mkdir(parents=True, exist_ok=False)
    cells = list(cells or required_cells())
    if len(cells) != len(set(cells)) or set(cells) - set(required_cells()):
        raise ValueError('duplicate/unknown performance cells')
    all_cells = [case['id'] + ':' + provider + suffix
                 for case in plan['cases'] for provider in ('local', 'ray', 'two_hosts')
                 for suffix in ((':p1', ':p2') if case['id'] == 'finite_sealed' else ('',))]
    report = {'schema': 'demiflow.performance.report.v1', 'required_cells': cells, 'trials': [],
              'warmup_separate': True, 'environment_cells_not_run': sorted(set(all_cells)-set(cells)),
              'measurements': [], 'plan': plan,
              'baseline_policy': json.loads(PLAN_PATH.read_text())['baseline_policy'],
              'historical_ray_data_performance': 'not_run; B1-Ray is not an old-Ray-Data performance comparison',
              'not_comparable_before_execution': []}
    for cell in cells:
        pieces = cell.split(':'); case_id, provider = pieces[:2]
        case = next(case for case in plan['cases'] if case['id'] == case_id)
        trial_settings = dict(settings, **environment_settings.get(provider, {}))
        if len(pieces) == 3: trial_settings['partitions'] = int(pieces[2][1:])
        baseline_role = expected_baseline(cell)
        baseline = baseline_roots.get(cell) or baseline_roots.get(baseline_role['key'])
        if baseline is None: continue
        variants = {'baseline': baseline, 'candidate': {'root': str(candidate_root), 'kind': 'candidate'}}
        # Reject the exposed local-NVMe versus shared-source comparison before
        # spending thirteen trials on it. Actual child/node probes remain
        # mandatory: this driver preflight cannot prove remote deployment.
        preflight = {arm: {role: filesystem_identity(path) for role, path in
                     _storage_locations(variant['root'], output_root).items()
                     if role in ('source_checkout', 'source_package')}
                     for arm, variant in variants.items()}
        valid = all(item.get('status') == 'observed' and item.get('path_exists') is True
                    for locations in preflight.values()
                    for item in locations.values())
        keys = [{role: {key: item.get(key) for key in _MOUNT_IDENTITY}
                 for role, item in locations.items()} for locations in preflight.values()]
        if not valid or keys[0] != keys[1]:
            report['not_comparable_before_execution'].append({'cell': cell,
                'reasons': ['driver source filesystem unknown or physically different across arms'],
                'evidence': preflight,
                'conclusion': 'not comparable; no business trial or relative ratio produced'})
            continue
        schedule = [('warmup', -1, arm) for arm in variants]
        for repeat in range(5):
            schedule += [('measure', repeat, arm) for arm in
                         (('baseline', 'candidate') if repeat % 2 == 0 else ('candidate', 'baseline'))]
        schedule += [('cancel', 0, 'candidate')]
        for phase, repeat, arm in schedule:
            selected = dict(variants[arm])
            selected.setdefault('python', settings.get('python', sys.executable))
            if arm == 'baseline':
                evidence = snapshot(selected['root'])
                if (evidence['head'] != baseline_role['commit'] or not evidence['production_clean']
                    or selected.get('kind') != baseline_role['kind']):
                    raise ValueError('baseline checkout/role differs from approved ' + baseline_role['label'])
            trial = measure_trial(case, selected, phase, repeat, provider,
                output_root / (cell.replace(':', '-') + f'-{phase}-{repeat}-{arm}'), **trial_settings)
            trial.update(cell=cell, arm=arm)
            if arm == 'baseline': trial['baseline_role'] = baseline_role
            report['trials'].append(trial)
            _write(output_root / 'report.json', report)
    report['measurements'] = [{'repetition': n, 'trials': [r for r in report['trials']
        if r.get('phase') == 'measure' and r.get('repetition') == n]} for n in range(5)]
    report['evaluation'] = evaluate_report(report)
    _write(output_root / 'report.json', report)
    return report


def performance(fx, fault=None):
    """Consumer of a completed, digest-bound formal report; never invent timings."""
    if fault is not None: raise ValueError('performance accepts measured cancellation trials, not an implicit fault substitute')
    path = os.environ.get('DEMIFLOW_PERFORMANCE_REPORT')
    if not path:
        from runtime_v3_support import NotRunError
        raise NotRunError('performance', {'missing': 'fixed candidate nine-class comparative report'})
    report = json.loads(Path(path).read_text())
    verdict = evaluate_report(report, required=required_cells())
    if verdict['status'] == 'failed': raise AssertionError(verdict)
    if verdict['status'] != 'passed':
        from runtime_v3_support import NotRunError
        raise NotRunError('performance', verdict)
    expected = snapshot(OWN_TREE)['production_sha256']
    manifest = snapshot(OWN_TREE)
    candidates = [r for r in report['trials'] if r['arm'] == 'candidate']
    if any(r['source_before']['production_sha256'] != expected or not r['source_unchanged']
           or any(r['source_before'].get(key) != manifest[key] for key in ('harness_sha256', 'plan_sha256'))
           for r in candidates):
        raise AssertionError('performance report belongs to different production bytes')
    return {'measurements': report['measurements'], 'warmup_separate': True,
            'report': str(Path(path).resolve()), 'evaluation': verdict,
            'child_execute_action_calls': [c for r in candidates for c in r['child_execute_action_calls']],
            'environment_cells_not_run': report['environment_cells_not_run']}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--child'); parser.add_argument('--preflight', nargs='+')
    parser.add_argument('--variant-root'); parser.add_argument('--kind', default='historical')
    parser.add_argument('--output'); args = parser.parse_args()
    if args.child:
        result = run_child(json.loads(Path(args.child).read_text()))
        return 0 if result['status'] in ('passed', 'unsupported', 'not_run') else 1
    if args.preflight:
        plan = json.loads(PLAN_PATH.read_text())
        failed = False
        for name in args.preflight:
            spec = next(c for c in plan['cases'] if c['id'] == name)
            result = measure_trial(spec, {'root': args.variant_root, 'kind': args.kind},
                'warmup', -1, 'local', Path(args.output) / name)
            failed = failed or result['status'] == 'failed'
            print(_json({'case': name, 'status': result['status'], 'elapsed_s': result.get('elapsed_s'),
                         'error': result.get('error', {}).get('message'), 'output': result.get('output')}), flush=True)
        return 1 if failed else 0
    parser.error('choose --child or --preflight')


if __name__ == '__main__':
    raise SystemExit(main())
