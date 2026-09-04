"""Deterministic backend-neutral physical plan construction."""
from __future__ import annotations
import hashlib,json
from dataclasses import replace
from .liveness import validate_resource_liveness
from .model import PhysicalPlan, PhysicalStage
from .policy import BUILT_IN_RULE_VERSION
from .traits import operation_traits,source_traits
from .model import ResourceBundle
from ..errors import PhysicalPlanningError


def plan_action(
    *, backend, action_kind, source, operations, terminal_node,
    terminal_traits, policy, snapshot, work_units=None, work_units_by_stage=None,
    terminal_native_options=None, parallelism_caps_by_stage=None,
):
    nodes=[(source,source_traits(source)),*((op,operation_traits(op)) for op in operations),(terminal_node,terminal_traits)]
    bounds=_normalize_work_units(len(nodes),work_units,work_units_by_stage)
    caps=_normalize_parallelism_caps(len(nodes),parallelism_caps_by_stage)
    stages=[]
    for ordinal,(node,logical_traits) in enumerate(nodes):
        native=terminal_native_options if node is terminal_node else getattr(node,"native_options",None)
        traits=_effective_traits(node,logical_traits,native,ordinal)
        resource=ResourceBundle() if traits.worker_model == "driver" else policy.worker(traits.policy_class)
        limit=policy.limit_for(traits.category)
        stage_bound=bounds[ordinal]
        maximum=max(1,min(limit,stage_bound if stage_bound is not None else limit))
        protocol_cap=caps[ordinal]
        if protocol_cap is not None:
            maximum=min(maximum,protocol_cap)
        minimum,initial=1,maximum
        exact=set()
        if native is not None:
            if native.backend != backend: raise PhysicalPlanningError("native_backend_mismatch",responsibility="candidate",stage_ordinal=ordinal)
            minimum=native.min_workers or minimum
            initial=native.initial_workers or maximum
            maximum=native.max_workers or maximum
            if maximum>limit: raise PhysicalPlanningError("native_policy_limit_exceeded",responsibility="candidate",stage_ordinal=ordinal)
            if protocol_cap is not None and max(minimum,initial,maximum)>protocol_cap:
                raise PhysicalPlanningError("protocol_parallelism_exceeded",responsibility="candidate",stage_ordinal=ordinal)
            values=resource.to_dict(); values.update(dict(native.worker_resources or {})); resource=ResourceBundle(**values)
            exact.update(key for key,value in (("min_workers",native.min_workers),("initial_workers",native.initial_workers),("max_workers",native.max_workers),("worker_resources",native.worker_resources)) if value is not None and value!={})
            if native.compute_kind:
                exact.update(("worker_model","lifecycle"))
            if not minimum <= initial <= maximum:
                raise PhysicalPlanningError("native_worker_range_invalid",responsibility="candidate",stage_ordinal=ordinal)
        stages.append(PhysicalStage(ordinal,type(node).__name__,node,traits,minimum,initial,maximum,resource,stage_bound,frozenset(exact)))
    def build(values):
        payload={
            "backend":backend,
            "action":action_kind,
            "stages":[{
                "ordinal":s.ordinal,
                "kind":s.kind,
                "worker_model":s.traits.worker_model,
                "lifecycle":s.traits.lifecycle,
                "workers":[s.min_workers,s.initial_workers,s.max_workers],
                "resources":s.worker_resources.to_dict(),
                "work_units_upper_bound":s.work_units_upper_bound,
                "native_exact_fields":sorted(s.native_exact_fields),
            } for s in values],
            "policy":policy.digest,
            "snapshot":snapshot.snapshot_id,
            "rules":BUILT_IN_RULE_VERSION,
        }
        pid=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(",",":")).encode()).hexdigest()
        resident=tuple(s.ordinal for s in values if s.traits.lifecycle=="resident" and s.traits.worker_model!="driver")
        frontiers=tuple(s.ordinal for s in values if s.traits.lifecycle!="resident" and s.traits.worker_model!="driver")
        groups=tuple(resident+(ordinal,) for ordinal in frontiers) or ((resident,) if resident else ())
        groups=tuple(group for group in groups if group)
        return PhysicalPlan(backend,action_kind,values[0],tuple(values[1:-1]),values[-1],(),groups,snapshot.snapshot_id,BUILT_IN_RULE_VERSION,policy.digest,pid)
    current=stages
    while True:
        candidate=build(current); report=validate_resource_liveness(candidate,snapshot,policy.resource_reserve)
        if report.safe:return candidate
        if report.code != "insufficient_resources_for_progress":
            responsibility="target" if report.code in {"resource_reserve_unavailable","resource_analysis_incomplete"} else "runtime"
            raise PhysicalPlanningError(report.code,responsibility=responsibility,stage_ordinal=report.stage_ordinal)
        changed=False
        for index in range(len(current)-1,-1,-1):
            stage=current[index]
            # This liveness model admits transient stages one progress worker
            # at a time. Reducing their concurrency ceiling cannot make an
            # unsafe admission safe; only resident initial footprint can.
            if stage.traits.lifecycle != "resident":
                continue
            if not stage.traits.elastic or stage.initial_workers<=stage.min_workers:
                continue
            new_initial=stage.initial_workers-1
            if "initial_workers" in stage.native_exact_fields:
                continue
            new_max=stage.max_workers
            if "max_workers" not in stage.native_exact_fields:
                new_max=min(new_max,new_initial)
            current=[*current]
            current[index]=replace(stage,initial_workers=new_initial,max_workers=new_max)
            changed=True
            break
        if not changed:
            responsibility="target" if report.code in {"resource_reserve_unavailable","resource_analysis_incomplete"} else "runtime"
            raise PhysicalPlanningError(report.code,responsibility=responsibility,stage_ordinal=report.stage_ordinal)


def _effective_traits(node, traits, native, ordinal):
    kind=getattr(native,"compute_kind","") if native is not None else ""
    if not kind:
        return traits
    if traits.worker_model == "driver":
        raise PhysicalPlanningError("native_compute_unsupported",responsibility="candidate",stage_ordinal=ordinal)
    if kind == "actor_pool":
        policy_class="network_io_worker" if traits.category=="network_io" else "reusable_worker"
        return replace(traits,worker_model="reusable",lifecycle="resident",policy_class=policy_class)
    if kind == "task_pool":
        callable_spec=getattr(node,"callable",None)
        if traits.category=="network_io" or callable_spec is None or callable_spec.is_class:
            raise PhysicalPlanningError("native_task_pool_incompatible",responsibility="candidate",stage_ordinal=ordinal)
        return replace(traits,worker_model="ephemeral",lifecycle="transient",policy_class="ephemeral_worker")
    raise PhysicalPlanningError("native_compute_kind_invalid",responsibility="candidate",stage_ordinal=ordinal)


def _normalize_work_units(stage_count, work_units, values):
    if values is not None:
        result=tuple(values)
        if len(result)!=stage_count:
            raise ValueError("work_units_by_stage must align with physical stages")
    else:
        result=(work_units,)*stage_count
    for item in result:
        if item is not None and (isinstance(item,bool) or not isinstance(item,int) or item<0):
            raise ValueError("work unit bounds must be non-negative integers or None")
    return result


def _normalize_parallelism_caps(stage_count, values):
    if values is None:
        return (None,) * stage_count
    result=tuple(values)
    if len(result)!=stage_count:
        raise ValueError("parallelism_caps_by_stage must align with physical stages")
    for item in result:
        if item is not None and (
            isinstance(item,bool) or not isinstance(item,int) or item<=0
        ):
            raise ValueError("parallelism caps must be positive integers or None")
    return result
