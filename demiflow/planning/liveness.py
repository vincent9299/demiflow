"""Conservative resource admission for a complete physical action."""
from __future__ import annotations
from dataclasses import dataclass
from .model import BackendResourceSnapshot, PhysicalPlan, ResourceBundle

@dataclass(frozen=True)
class ResourceLivenessReport:
    safe: bool
    code: str = ""
    stage_ordinal: int = -1

def validate_resource_liveness(plan: PhysicalPlan, snapshot: BackendResourceSnapshot, reserve: ResourceBundle) -> ResourceLivenessReport:
    if not snapshot.analysis_complete: return ResourceLivenessReport(False,"resource_analysis_incomplete")
    capacity=snapshot.aggregate
    if not reserve.fits(capacity): return ResourceLivenessReport(False,"resource_reserve_unavailable")
    usable=ResourceBundle(capacity.cpu-reserve.cpu,capacity.gpu-reserve.gpu,max(0,capacity.memory_bytes-reserve.memory_bytes),{k:max(0,v-reserve.custom_resources.get(k,0)) for k,v in capacity.custom_resources.items()})
    by_ordinal={stage.ordinal:stage for stage in plan.stages}
    for stage in plan.stages:
        if stage.min_workers < 1 or not stage.min_workers <= stage.initial_workers <= stage.max_workers:
            return ResourceLivenessReport(False,"invalid_worker_range",stage.ordinal)
        if stage.traits.worker_model != "driver" and stage.traits.lifecycle == "resident" and stage.traits.worker_model != "reusable":
            return ResourceLivenessReport(False,"invalid_resident_worker_model",stage.ordinal)
        if stage.traits.worker_model != "driver" and stage.traits.lifecycle != "resident" and stage.traits.worker_model == "reusable":
            return ResourceLivenessReport(False,"invalid_reusable_lifecycle",stage.ordinal)
        if stage.traits.worker_model != "driver" and not any(stage.worker_resources.fits(node) for node in snapshot.nodes): return ResourceLivenessReport(False,"worker_not_placeable",stage.ordinal)
    for group in plan.concurrency_groups:
        required=ResourceBundle()
        for ordinal in group:
            stage=by_ordinal[ordinal]
            # Admission must prove the footprint that lowering will submit.
            # Reusable pools are created at initial_workers, not min_workers.
            count=stage.initial_workers if stage.traits.lifecycle=="resident" else 1
            required=required.plus(stage.worker_resources.scale(count))
        if not required.fits(usable): return ResourceLivenessReport(False,"insufficient_resources_for_progress",group[0] if group else -1)
    return ResourceLivenessReport(True)
