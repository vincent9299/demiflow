"""Read-only, bounded sampling of original Ray business allocations.

These actors are measurement instruments, never schedulers or allocators.  Each
node samples only PIDs registered through the original journal, worker identity,
and GCS actor record. Shared Ray daemons/object-store memory is not attributed to
the business RSS; process RSS itself can count shared pages more than once.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import threading
import time


def _birth(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except (FileNotFoundError, ProcessLookupError):
        return None


def _process_tree(pid, birth):
    """A PID is never reused as an identity; retain every observed birth."""
    found, queue = {}, [(pid, birth)]
    while queue:
        current, expected = queue.pop()
        if current in found or _birth(current) != expected:
            continue
        if len(found) >= 512:
            raise ValueError('Ray observed business process cap exceeded')
        found[current] = expected
        try:
            with os.scandir(f'/proc/{current}/task') as tasks:
                for number, task in enumerate(tasks, 1):
                    if number > 4096:
                        raise ValueError('Ray observed business thread cap exceeded')
                    try:
                        children = Path(task.path, 'children').read_text().split()
                        for child in children:
                            child = int(child)
                            child_birth = _birth(child)
                            if child_birth is not None:
                                queue.append((child, child_birth))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
        except (FileNotFoundError, ProcessLookupError):
            pass
    return found


def _metrics(pid, birth):
    if _birth(pid) != birth:
        return 0, 0
    rss, fds = 0, 0
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith('VmRSS:'):
                rss = int(line.split()[1]) * 1024
        with os.scandir(f'/proc/{pid}/fd') as entries:
            for fds, _ in enumerate(entries, 1):
                if fds > 65536:
                    raise ValueError('Ray observed business FD cap exceeded')
    except (FileNotFoundError, ProcessLookupError):
        return 0, 0
    # Recheck after reads, so a recycled PID cannot donate unrelated metrics.
    return (rss, fds) if _birth(pid) == birth else (0, 0)


def _original_allocation(path, allocation_id):
    with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        row = db.execute('SELECT scope_id,spec_hash,cleanup,document FROM resource_allocations '
                         'WHERE allocation_id=?', (allocation_id,)).fetchone()
    if row is None or len(row[3]) > 1024 * 1024:
        raise AssertionError('original bounded resource allocation is missing')
    return {'scope_id': row[0], 'spec_hash': row[1], 'cleanup': row[2],
            'document': json.loads(row[3])}


class NodeSampler:
    """One test-owned zero-CPU actor per actual node, 20 Hz bounded /proc reads."""
    def __init__(self, expected_root, expected_node, expected_domain):
        import demiflow
        import ray
        self.node_id = ray.get_runtime_context().get_node_id()
        assert self.node_id == expected_node
        self.source = str(Path(demiflow.__file__).resolve())
        assert Path(self.source).is_relative_to(Path(expected_root).resolve())
        assert os.environ['DEMIFLOW_RESOURCE_ROOT'] == expected_domain
        self.root = expected_root
        self.domain = expected_domain
        self.entries, self.known, self.errors = {}, {}, []
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.samples, self.scan_ns = 0, 0
        self.peaks = {'rss_bytes': 0, 'fd_count': 0, 'processes': 0}
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def ready(self):
        import ray
        return {'node_id': self.node_id, 'hostname': socket.gethostname(),
                'pid': os.getpid(), 'birth': _birth(os.getpid()), 'source': self.source,
                'job_id': ray.get_runtime_context().get_job_id(),
                'resource_domain': self.domain}

    def register(self, evidence):
        import ray
        info = ray._private.state.actors(evidence['actor_id'])
        assert info and info['State'] == 'ALIVE'
        assert info['Address']['NodeID'] == self.node_id == evidence['node_id']
        assert info['Name'] == 'demiflow-worker-' + evidence['allocation_id']
        assert info['Pid'] == evidence['pid'] and info['JobID'] == evidence['job_id']
        assert info['JobID'] == ray.get_runtime_context().get_job_id()
        pid = evidence['pid']
        birth = _birth(pid)
        assert birth is not None, 'original business actor has already disappeared'
        # Read only the two selected environment values, never credentials.
        with open(f'/proc/{pid}/environ', 'rb') as stream:
            raw = stream.read(1024 * 1024 + 1)
        assert len(raw) <= 1024 * 1024
        allowed = {b'PYTHONPATH', b'DEMIFLOW_RESOURCE_ROOT'}
        environment = {key.decode(): value.decode() for field in raw.split(b'\0')
                       if b'=' in field for key, value in [field.split(b'=', 1)] if key in allowed}
        assert self.root in environment.get('PYTHONPATH', '').split(os.pathsep)
        assert environment.get('DEMIFLOW_RESOURCE_ROOT') == self.domain
        assert _birth(pid) == birth
        with self.lock:
            if len(self.entries) >= 512:
                raise ValueError('Ray allocation observation cap exceeded')
            key = evidence['allocation_id']
            previous = self.entries.get(key)
            if previous:
                assert previous['pid'] == pid and previous['birth'] == birth
                return previous
            self.entries[key] = {**evidence, 'birth': birth, 'sample_count': 0,
                'rss_peak_bytes': 0, 'fd_peak': 0, 'process_peak': 0,
                'environment': environment}
            self.known[key] = {}
        self.sample()
        return self.entries[key].copy()

    def sample(self):
        started = time.monotonic_ns()
        with self.lock:
            try:
                aggregate = {}
                for key, entry in self.entries.items():
                    found = _process_tree(entry['pid'], entry['birth'])
                    self.known[key].update(found)
                    if len(self.known[key]) > 512:
                        raise ValueError('Ray historical process observation cap exceeded')
                    alive = {pid: birth for pid, birth in self.known[key].items()
                             if _birth(pid) == birth}
                    metrics = {pid: _metrics(pid, birth) for pid, birth in alive.items()}
                    aggregate.update(metrics)
                    if metrics:
                        entry['sample_count'] += 1
                    entry['rss_peak_bytes'] = max(entry['rss_peak_bytes'], sum(m[0] for m in metrics.values()))
                    entry['fd_peak'] = max(entry['fd_peak'], sum(m[1] for m in metrics.values()))
                    entry['process_peak'] = max(entry['process_peak'], len(metrics))
                for key, value in (('rss_bytes', sum(m[0] for m in aggregate.values())),
                                   ('fd_count', sum(m[1] for m in aggregate.values())),
                                   ('processes', len(aggregate))):
                    self.peaks[key] = max(self.peaks[key], value)
                self.samples += 1
            except Exception as exc:
                if len(self.errors) < 20:
                    self.errors.append(repr(exc))
            self.scan_ns += time.monotonic_ns() - started

    def _loop(self):
        while not self.stopped.wait(.05):
            self.sample()

    def result(self):
        self.sample()
        with self.lock:
            return {**self.ready(), 'sampled_peaks': dict(self.peaks), 'samples': self.samples,
                    'sampling_wall_s': self.scan_ns / 1e9, 'errors': list(self.errors),
                    'allocations': [dict(entry, process_births=sorted(self.known[key].items()),
                        remaining=[pid for pid, birth in self.known[key].items() if _birth(pid) == birth])
                        for key, entry in self.entries.items()]}

    def close(self):
        self.stopped.set()
        self.thread.join(5)
        assert not self.thread.is_alive()
        return self.result()


def _probe_exit(pid, birth):
    return {'pid': pid, 'birth': birth, 'same_process_alive': _birth(pid) == birth,
            'hostname': socket.gethostname()}


class RayObservations:
    def __init__(self, expected_root, domain, *, require_two_hosts=False):
        self.expected_root, self.domain = str(expected_root), str(domain)
        self.require_two_hosts = require_two_hosts
        self.monitors, self.ready, self.allocations = {}, {}, {}
        self.registration_s, self.preparation_s = 0., 0.
        self.result = None

    def __enter__(self):
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        started = time.monotonic()
        nodes = [node for node in ray.nodes() if node['Alive']]
        assert 1 <= len(nodes) <= 8, 'owned performance node count must be bounded'
        if self.require_two_hosts:
            assert len({node['NodeManagerHostname'] for node in nodes}) == 2
        actor_type = ray.remote(num_cpus=0, num_gpus=0, max_restarts=0, max_task_retries=0)(NodeSampler)
        try:
            for node in nodes:
                node_id = node['NodeID']
                monitor = actor_type.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)).remote(
                    self.expected_root, node_id, self.domain)
                self.monitors[node_id] = monitor
            for node_id, actor in self.monitors.items():
                self.ready[node_id] = ray.get(actor.ready.remote(), timeout=30)
        except BaseException:
            for actor in self.monitors.values():
                ray.kill(actor, no_restart=True)
            raise
        self.preparation_s = time.monotonic() - started
        return self

    def register(self, execution, lease):
        import ray
        started = time.monotonic()
        if lease.allocation_id in self.allocations:
            return
        assert execution._policy.provider == 'ray_core'
        actor = execution._endpoints[lease.allocation_id]._connector._actor(lease.allocation_id)
        actor_id = actor._actor_id.hex()
        info = ray._private.state.actors(actor_id)
        node_id = lease.host_id.removeprefix('ray-')
        assert info and info['Address']['NodeID'] == node_id and node_id in self.monitors
        assert info['Name'] == 'demiflow-worker-' + lease.allocation_id
        live = ray.get(actor.identity.remote(), timeout=10)
        journal_path = str(execution._action.context.journal.path)
        original = _original_allocation(journal_path, lease.allocation_id)
        assert live['boot_id'] == lease.boot_id == original['document']['boot_id']
        assert live['spec_hash'] == original['spec_hash']
        placement = original['document'].get('placement') or {}
        if placement.get('actor_id'):
            assert placement['actor_id'] == actor_id
        evidence = {'allocation_id': lease.allocation_id, 'boot_id': lease.boot_id,
                    'spec_hash': live['spec_hash'], 'scope_id': original['scope_id'],
                    'stage_id': lease.stage_id, 'journal_path': journal_path,
                    'actor_id': actor_id, 'node_id': node_id, 'job_id': info['JobID'],
                    'pid': info['Pid'], 'original_placement': placement}
        self.allocations[lease.allocation_id] = evidence
        ray.get(self.monitors[node_id].register.remote(evidence), timeout=10)
        self.registration_s += time.monotonic() - started

    def __exit__(self, *args):
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        result = {'scope': 'original Ray allocations, actual node-local PID/birth trees; sampled RSS/FD',
            'excluded': ['shared Ray daemons/object-store memory', 'monitor/probe actors',
                         'node-local worker artifact-cache disk outside the owned trial directory'],
            'preparation_s_outside_business_clock': self.preparation_s,
            'registration_s_inside_business_clock': self.registration_s,
            'errors': [], 'nodes': [], 'original_cleanup': [], 'monitor_exit': [],
            'journal_allocation_ids': []}
        try:
            # Do not certify merely the subset which reached the activation
            # observer. Unobserved original allocations make coverage fail.
            for path in sorted({entry['journal_path'] for entry in self.allocations.values()}):
                with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as db:
                    rows = db.execute('SELECT allocation_id FROM resource_allocations ORDER BY allocation_id').fetchmany(513)
                if len(rows) > 512:
                    raise ValueError('Ray original allocation evidence cap exceeded')
                result['journal_allocation_ids'].extend(row[0] for row in rows)
            deadline = time.monotonic() + 10
            while True:
                alive = [entry for entry in self.allocations.values()
                         if (ray._private.state.actors(entry['actor_id']) or {}).get('State') != 'DEAD']
                if not alive or time.monotonic() >= deadline:
                    break
                time.sleep(.05)
            for entry in self.allocations.values():
                original = _original_allocation(entry['journal_path'], entry['allocation_id'])
                info = ray._private.state.actors(entry['actor_id'])
                result['original_cleanup'].append({'allocation_id': entry['allocation_id'],
                    'cleanup': original['cleanup'], 'actor_state': info.get('State') if info else None})
            for node_id, actor in self.monitors.items():
                result['nodes'].append(ray.get(actor.close.remote(), timeout=10))
        except Exception as exc:
            result['errors'].append(repr(exc))
        finally:
            # These are solely the instrument actors created by this instance.
            for actor in self.monitors.values():
                ray.kill(actor, no_restart=True)
            probe = ray.remote(num_cpus=0, num_gpus=0, max_retries=0)(_probe_exit)
            for node_id, ready in self.ready.items():
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        value = ray.get(probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)).remote(
                            ready['pid'], ready['birth']), timeout=10)
                        if not value['same_process_alive'] or time.monotonic() >= deadline:
                            break
                        time.sleep(.05)
                    result['monitor_exit'].append(value)
                except Exception as exc:
                    result['errors'].append(repr(exc))
            self.result = result
        result['errors'].extend(ray_receipt_errors(result, require_two_hosts=self.require_two_hosts))


def ray_receipt_errors(result, *, require_two_hosts=False):
    if not result:
        return ['Ray allocation samples absent']
    errors = list(result.get('errors', ()))
    nodes = result.get('nodes', [])
    allocations = [entry for node in nodes for entry in node.get('allocations', [])]
    if not allocations or len({entry['allocation_id'] for entry in allocations}) != len(allocations):
        errors.append('missing or duplicate original Ray allocation samples')
    declared = result.get('journal_allocation_ids', [])
    if (len(declared) != len(set(declared))
        or set(declared) != {entry['allocation_id'] for entry in allocations}):
        errors.append('original journal contains unobserved or duplicate Ray allocations')
    if any(node.get('errors') or node.get('samples', 0) < 1 for node in nodes):
        errors.append('Ray node samples incomplete')
    if any(entry.get('sample_count', 0) < 1 or entry.get('rss_peak_bytes', 0) <= 0
           or entry.get('fd_peak', 0) <= 0 or entry.get('remaining') != []
           or not entry.get('birth') or not entry.get('process_births') for entry in allocations):
        errors.append('Ray business PID samples or physical exit incomplete')
    cleanup = result.get('original_cleanup', [])
    if ({entry['allocation_id'] for entry in cleanup} != {entry['allocation_id'] for entry in allocations}
        or any(entry['cleanup'] != 'CONFIRMED' or entry['actor_state'] != 'DEAD' for entry in cleanup)):
        errors.append('original Ray journal cleanup and actor death incomplete')
    if len(result.get('monitor_exit', [])) != len(nodes) or any(entry['same_process_alive'] for entry in result.get('monitor_exit', [])):
        errors.append('test-owned monitor physical exit incomplete')
    if require_two_hosts and len({node['hostname'] for node in nodes if node.get('allocations')}) != 2:
        errors.append('business did not execute on both actual physical hosts')
    return errors
