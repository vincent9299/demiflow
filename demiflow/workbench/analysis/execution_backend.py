"""One internal backend contract; sessions/outputs/Case keep their sole ledger."""
from __future__ import annotations

from typing import Protocol

from demiflow.workbench.analysis import contracts as c

RECEIPT = 'demiflow_analysis_cleanup_receipt_v1'
CAPABILITIES = 'demiflow_analysis_execution_capabilities_v1'


def capabilities(mode):
    c.one_of(mode, {'trusted_local','oci'}, 'execution mode')
    isolated = mode == 'oci'
    return dict(format=CAPABILITIES, mode=mode, backend_revision='1',
        filesystem_isolation=isolated, network_isolation=isolated,
        hard_memory_quota=isolated, hard_disk_quota=False,
        malicious_descendant_containment=isolated, independent_force_stop=isolated,
        cleanup_scope='owned_cgroup' if isolated else 'managed_processes',
        resource_observation='cgroup_and_sampled' if isolated else 'sampled',
        source_tracking='registered_broker_dependencies')


def cleanup_receipt(*, job_id, mode, state, processes, descendants, channels, temporary, reason=None):
    c.one_of(mode, {'trusted_local','oci'}, 'cleanup mode')
    c.one_of(state, {'pending','unknown','complete'}, 'cleanup state')
    flags = (processes, descendants, channels, temporary)
    if any(type(flag) is not bool for flag in flags): c.reject('Cleanup facts require explicit booleans')
    if state == 'complete' and not all(flags): c.reject('Complete cleanup requires every scoped fact')
    return dict(format=RECEIPT,job_id=job_id,backend_mode=mode,
        scope='managed_processes' if mode=='trusted_local' else 'owned_cgroup',state=state,
        managed_processes_exited=processes,known_descendants_exited=descendants,
        channels_closed=channels,temporary_files_removed=temporary,reason=reason)


class ExecutionBackend(Protocol):
    root: object
    leases: dict
    def describe(self) -> dict: ...
    def create(self, *, project, execution_id, role, config, web_parent, owner_digest,
               authorize, origin_digest, read_owner=None, workspace=None) -> dict: ...
    def start(self, identifier, *, authorize) -> dict: ...
    def register_bridge(self, identifier, path) -> dict: ...
    def accept_peer(self, identifier, peer, *, init_only=False) -> bool: ...
    def observe(self, identifier) -> dict: ...
    def stop(self, identifier, *, reason) -> dict: ...
    def cleanup(self, identifier, *, close_bridges, clean_private_io=None) -> dict: ...
    def retire_read(self, identifier) -> dict: ...
    def owner(self, operation, digest, *, record=None) -> dict: ...
    def read_owner(self, operation, digest, *, record=None) -> dict: ...
    def close(self) -> None: ...


class OciBackend:
    """Adapter only; strict original Runtime/WatchClient still admit OCI."""
    mode = 'oci'

    def __init__(self, runtime, watchdog):
        from demiflow.workbench.analysis.runtime import Runtime
        if not isinstance(runtime, Runtime): c.reject('Admitted OCI Runtime required')
        self.runtime, self.watchdog = runtime, watchdog

    @property
    def root(self): return self.runtime.root
    @property
    def leases(self): return self.runtime.leases
    def describe(self): return capabilities(self.mode)
    def owner(self, operation, digest, *, record=None):return self.watchdog.owner(operation,digest,record=record)
    def read_owner(self, operation, digest, *, record=None):return self.watchdog.read_owner(operation,digest,record=record)
    def create(self, *, origin_digest, workspace=None, **kwargs):
        c.sha(origin_digest, 'generation execution origin')
        value=self.runtime.create(**kwargs)
        return dict(job_id=value['job_id'],state=value['phase'],execution_mode=self.mode,origin_digest=origin_digest)
    def start(self, identifier, *, authorize):
        value=self.runtime.start(identifier,authorize=authorize)
        return dict(job_id=value['job_id'],state=value['phase'])
    def register_bridge(self, identifier, path):return dict(registration='controller_owned',backend_mode=self.mode)
    def accept_peer(self, identifier, peer, *, init_only=False):
        lease=self.leases[identifier];self.runtime.group(lease)
        return peer.job(self.runtime.deployment.paths.delegated_cgroup,lease,init_only=init_only)
    def observe(self, identifier):
        from demiflow.forge.schema import ForgeError
        lease=self.leases[identifier]
        try:
            if lease['state']=='cleanup_complete': return dict(state='exited',reason=None,status=None)
            self.runtime.group(lease)
            return dict(state='alive' if self.runtime.groups.populated(identifier,lease['cgroup']['cgroup_identity']) else 'exited',reason=None,status=None)
        except (ForgeError,OSError): return dict(state='unknown',reason='original_oci_observation_unavailable',status=None)
    def stop(self, identifier, *, reason):
        result=self.runtime.stop(identifier,reason=reason)
        complete=result['cleanup_proven'] is True
        if complete and self.watchdog.register(self.leases[identifier]) is not True:
            c.reject('Independent original job cleanup acknowledgement is unavailable',code='busy')
        return dict(job_id=identifier,accepted=result['accepted'],cleanup_complete=complete,cleanup_proven=complete)
    def cleanup(self, identifier, *, close_bridges, clean_private_io=None):
        result=self.runtime.cleanup(identifier,close_bridges=close_bridges,clean_private_io=clean_private_io)
        complete=result['cleanup_proven'] is True
        if complete and self.watchdog.register(self.leases[identifier]) is not True:
            c.reject('Independent original job cleanup acknowledgement is unavailable',code='busy')
        receipt=cleanup_receipt(job_id=identifier,mode=self.mode,state='complete' if complete else 'pending',
            processes=complete,descendants=complete,channels=complete,temporary=complete)
        return {**result,'cleanup_complete':complete,'cleanup_receipt':receipt}
    def retire_read(self, identifier): return self.runtime.retire_read(identifier)
    def close(self): self.runtime.close()
