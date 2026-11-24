"""The engine-managed implementation of the training job port."""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
import time

from demiflow.resources.contracts import RESOURCE_PROTOCOL_VERSION, ResourceProfile, WorkerLaunchSpec
from demiflow.execution.contracts.dataflow_codec import encode_wire
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.training import FitRequest, FitResult, JobLookup, JobRef, TrainingSnapshotRef
from demiflow.execution.scheduler.native_limits import admission_timeout_ms
from demiflow.execution.contracts.recovery_contract import document, identifier, identity
from demiflow.execution.scheduler.job import JobAllocation
from demiflow.execution.storage.state_artifacts import atomic_bytes
from demiflow.execution.storage.model_store import read_metadata
from demiflow.execution.journal.training_index import TrainingIndex


class ManagedFitHost:
    def __init__(self, *, action, occurrence, binding, job_resources, timeout_ms,
                 services=None, scheduler=None, sealed=None, deadline=None):
        self.action, self.run = action, action.context
        self.occurrence, self.binding = occurrence, binding
        self.services, self.scheduler, self.sealed = services, scheduler, sealed
        self._job_resources = job_resources
        self._timeout, self._deadline = timeout_ms, deadline
        self._allocations = {}

    @classmethod
    def recover(cls, action, occurrence, binding, *, deadline):
        from demiflow.execution.hosts.configuration import original_job_resources
        run = action.context
        return cls(action=action, occurrence=occurrence, binding=binding,
                   job_resources=lambda scope: original_job_resources(run, scope), deadline=deadline,
                   timeout_ms=max(1, int(run.limits['cancel_grace_s'] * 1000)))

    def _remaining_ms(self):
        if self._deadline is None:
            return self._timeout
        remaining = int((self._deadline - time.monotonic()) * 1000)
        if remaining <= 0:
            raise TimeoutError("Managed job inspection deadline expired")
        return min(remaining, self._timeout)

    def _allocation(self, key):
        identifier(key)
        if key not in self._allocations:
            if self._job_resources is None:
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'This deployment has no group resource provider')
            self._allocations[key] = JobAllocation(self.run, key, self._job_resources('fit-' + key))
        return self._allocations[key]

    def _directory(self, key):
        return self.run.directory / 'training' / identifier(key)

    def submit(self, request, binding, request_key):
        if self.services is None:
            raise RuntimeError("Maintenance can only reconcile an existing job")
        try:
            return self._admit(request, binding, request_key)
        except Exception as error:
            # Failure to observe the original mutation remains uncertain. Only
            # an authoritative empty allocation rules out a possible spawn.
            if not self._allocation(request_key).accepted():
                from demiflow.execution.contracts.training import FitSubmissionRejected
                raise FitSubmissionRejected(str(error)) from error
            raise

    def _admit(self, request, binding, request_key):
        if binding != self.binding:
            raise ValueError("Managed host deployment differs")
        topology = request.semantics['topology']
        if topology.get('nodes', 1) != 1 or self.services.policy.provider != 'local':
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'This deployment supports one process host per job')
        options = binding.provider_options
        requests = {key: options[key] for key in ('cpu_millis','memory_bytes')}
        gpus = topology.get('gpus_per_node', 0)
        if type(gpus) is not int or gpus < 0:
            raise ValueError("Invalid GPUs per training host")
        if gpus:
            requests['accelerators'] = [{'type': 'gpu', 'count': gpus}]
        executable = Path(options.get('python_executable', sys.executable))
        if not executable.is_absolute() or not executable.is_file():
            raise ValueError("Managed trainer requires its deployment Python executable")
        self._launch(request, request_key, requests, executable)
        return JobRef(binding.provider_scope, request_key)

    def _launch(self, request, request_key, resources, executable, *, inspect_source=None):
        key = self._inspection_key(request_key) if inspect_source else request_key
        binding = self.binding
        root = self._directory(key)
        location = root / 'launch.json'
        launch = dict(work_dir=str(root), action=self.action.action_id, occurrence=self.occurrence,
                      request_key=request_key, request=request.to_dict(), binding=binding.to_dict(),
                      max_rss_bytes=self.run.limits['max_rss_bytes'], cancel_grace_s=self.run.limits['cancel_grace_s'],
                      bounds=asdict(self.services.bounds), store_root=str(self.services.store.root),
                      sealed=json.loads(encode_wire(self.sealed, type_name='SealedExecution', bounds=self.services.bounds)))
        if inspect_source:
            launch['inspect_source'] = inspect_source
        if location.exists():
            raw = read_metadata(location, 1024 * 1024)
            previous = json.loads(raw)
            launch['deadline_unix'] = previous['deadline_unix']
            if launch != previous:
                raise ValueError("Original managed launch changed")
        else:
            launch['deadline_unix'] = min(self.run.deadline, time.time() + self.services.bounds.timeout_ms / 1000)
            raw = document(launch)
            atomic_bytes(location, raw)
        digest = hashlib.sha256(raw).hexdigest()
        authority = dict(action=self.action.action_id, occurrence=self.occurrence,
                         instance=request.instance_id, request_key=request_key, provider_scope=binding.provider_scope,
                         digest=identity([request.to_dict(), binding.to_dict()]), launch_digest=digest)
        if inspect_source:
            authority['inspection_key'] = key
        self.run.journal.bind_source(self.run.fence, 'fit-resource-' + key, authority)
        module = 'demiflow.execution.hosts.training_bootstrap'
        spec = WorkerLaunchSpec({'launch': {'digest': digest, 'location': str(location)}}, module,
                        (str(executable), '-B', '-m', module, str(location), digest),
                        'training', {}, RESOURCE_PROTOCOL_VERSION)
        allocation = self._allocation(key)
        allocation.admit(self.scheduler, self.occurrence, ResourceProfile(resources), spec,
                         authority=authority, timeout_ms=admission_timeout_ms(self.services.policy))

    def lookup(self, request_key):
        allocation = self._allocation(request_key)
        evidence = dict(scope=self.binding.provider_scope, request_key=request_key)
        if allocation.accepted():
            return JobLookup('found', JobRef(self.binding.provider_scope, request_key), evidence)
        # No accepted mutation means no ensure_present could have run. The
        # journal's launch key and deterministic allocation are never recycled.
        return JobLookup('confirmed_absent', evidence={**evidence, 'authoritative': True,
                          'idempotency_expires_at': time.time() + self._remaining_ms() / 1000})

    def _key(self, job):
        if job.scope != self.binding.provider_scope:
            raise ValueError("Job belongs to another deployment")
        return identifier(job.job_id)

    def inspect(self, job):
        key = self._key(job)
        outcome = self._unit_status(key)
        if outcome != 'interrupted' or not self.binding.max_resumes:
            return outcome
        # Native optimizers may commit a directory before reporting its step.
        # Inspect the immutable, exited launch in an ordinary fenced CPU unit.
        # Stopped maintenance may only observe already admitted work.
        inspection_key = self._inspection_key(key)
        allocation = self._allocation(inspection_key)
        if not allocation.accepted():
            if self.services is None:
                return outcome
            original = self._original_launch(key)
            request = FitRequest.from_dict(original['request'])
            options = self.binding.provider_options
            resources = {name: options[name] for name in ('cpu_millis', 'memory_bytes')}
            self._launch(request, key, resources, options.get('python_executable', sys.executable),
                         inspect_source={'key': key, 'launch_digest': self._authority(key)['launch_digest']})
        inspected = self._unit_status(inspection_key)
        if inspected == 'succeeded':
            return 'interrupted'
        if inspected == 'interrupted':
            return 'failed'
        return inspected

    @staticmethod
    def _inspection_key(key):
        return identity(['fit.snapshot_inspection.v1', key])

    def _authority(self, key):
        return self.run.journal.source_binding('fit-resource-' + key)

    def _original_launch(self, key):
        raw = read_metadata(self._directory(key) / 'launch.json', 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != self._authority(key)['launch_digest']:
            raise ValueError("Original launch cannot be authenticated")
        return json.loads(raw)

    def _unit_status(self, key):
        root = self._directory(key)
        allocation = self._allocation(key)
        closed = allocation.manager.state.cleanup_complete(allocation.scope_id)
        if not closed:
            phase = allocation.refresh(deadline=time.monotonic() + self._remaining_ms() / 1000,
                                       observe_only=self.services is None)
            if phase in ('requested', 'starting', 'pending', 'reserved'):
                return 'pending'
            if phase in ('unknown', 'lost', 'terminating'):
                return 'unknown'
            if phase == 'running':
                if self.services is not None:
                    self._activate(key, root)
                return 'running'
            if phase not in ('failed', 'terminated'):
                return 'unknown'
            if not allocation.retire(cancelled=False, timeout_ms=self._remaining_ms()):
                return 'unknown'
        # A job-level close cannot replace proof from the original device pool.
        allocation.allocated_gpu_seconds()
        if (root / 'cancel').exists():
            return 'cancelled'
        if (root / 'result.json').exists():
            return 'succeeded'
        if (root / 'error.json').exists():
            return 'failed'
        return 'interrupted'

    def _activate(self, key, root):
        ready = root / 'ready.json'
        if not ready.exists() or (root / 'activate.json').exists():
            return
        authority = self._authority(key)
        proof = json.loads(read_metadata(ready, 4096))
        if proof != {'request_key': authority['request_key'], 'launch_digest': authority['launch_digest']}:
            raise ValueError("Training ready evidence differs from its original launch")
        with self.run.journal.fit_guard(self.run.fence, self.action.action_id, self.occurrence, admitting=True):
            active = TrainingIndex(self.binding, readonly=True).launch_is_active(
                authority['instance'], authority['request_key'], run_id=self.run.ref.run_id,
                action=self.action.action_id, occurrence=self.occurrence, attempt=self.run.attempt)
            if active and not (root / 'cancel').exists():
                atomic_bytes(root / 'activate.json', document(proof))

    def allocated_gpu_seconds(self, request_keys):
        total = 0.0
        for key in request_keys:
            seconds = self._allocation(key).allocated_gpu_seconds()
            if seconds is None:
                return None
            total += seconds
        return total

    def result(self, job):
        return FitResult.from_dict(json.loads(read_metadata(self._directory(self._key(job)) / 'result.json',
                                                             1024 * 1024)))

    def completed_snapshots(self, job):
        key = self._key(job)
        if self.inspect(job) != 'interrupted':
            raise ValueError("Snapshot reconciliation requires confirmed group exit")
        root = self._directory(key)
        request = FitRequest.from_dict(self._original_launch(key)['request'])
        reported = {}
        for step in request.semantics['save_steps']:
            path = root / 'reported' / (str(step) + '.json')
            if path.exists():
                snap = TrainingSnapshotRef.from_dict(json.loads(read_metadata(path, 1024 * 1024)))
                if snap.step != step:
                    raise ValueError("Snapshot report names a different step")
                reported[step] = snap
        inspected = self._directory(self._inspection_key(key)) / 'result.json'
        if inspected.exists():
            native = json.loads(read_metadata(inspected, 1024 * 1024))['snapshot']
            if native is not None:
                snapshot = TrainingSnapshotRef.from_dict(native)
                if snapshot.step not in request.semantics['save_steps'] or (
                        snapshot.step in reported and reported[snapshot.step] != snapshot):
                    raise ValueError("Native snapshot contradicts the declared or reported step")
                reported[snapshot.step] = snapshot
        return tuple(reported[step] for step in sorted(reported))

    def latest_snapshot(self, job):
        steps = self.completed_snapshots(job)
        return steps[-1] if steps else None

    def cancel(self, job):
        key = self._key(job)
        for unit in (key, self._inspection_key(key)):
            allocation = self._allocation(unit)
            if allocation.accepted():
                atomic_bytes(self._directory(unit) / 'cancel', b'cancel')
                allocation.retire(cancelled=True, timeout_ms=self._remaining_ms())
