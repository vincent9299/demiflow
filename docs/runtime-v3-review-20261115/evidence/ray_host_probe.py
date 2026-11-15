from pathlib import Path
import tempfile, json
import ray
import demiflow
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.dataflow_api import make_action_request
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.dataflow_services import make_driver_services
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.resources.contracts import ResourceScope
ROOT = Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115')
assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
class VerificationOnly(Exception): pass
root = Path(tempfile.mkdtemp(prefix='demiflow-gpt-ray-host-'))
try:
    ray.init(num_cpus=4, include_dashboard=False, _temp_dir=str(root / 'ray'), object_store_memory=100*1024**2)
    try:
        with RunContext.start(root / 'run') as run:
            ds = data.from_items([{'x': 1}]).map(lambda r: r)
            request = make_action_request(RayDatasetExecutor(), ds._source, ds._plan, kind='take', arguments={'limit': 1})
            policy, bounds = resolve_runtime_policy({}, run.limits, provider='ray_core')
            services = make_driver_services(request.action, policy=policy, bounds=bounds)
            ex = DataflowExecution(request, services=services)
            try:
                ex.prepare()
                scope = ex._scheduler.scope_id
                services.resources.state.open_scope(ex._fence(), ResourceScope(scope, ex._run_id(), {'provider': 'ray_core'}, ex._fence(),
                    {'cpu_millis': 4000, 'memory_bytes': bounds.max_resident_bytes, 'max_workers': 4}))
                ex._scope_open = True
                node = next(n for n in ex._graph.nodes if n.kind == 'transform')
                stage = next(s for s in ex._physical.stages if node.occurrence_id in s.occurrences)
                lease = ex._scheduler.place(ex._initial_work(stage, stage.partitions[0]), prepare=ex._prepare_launch, host_id=services.hosts[0].host_id)
                endpoint = ex._endpoints[lease.allocation_id]
                endpoint.activate(ex._assignment(lease, stage))
                assert endpoint._activated
                endpoint.cancel('host verification complete', deadline_ms=5000)
                print(json.dumps({'ray': ray.__version__, 'activated': True, 'host_count': len(services.hosts), 'full_business_run': False}), flush=True)
            finally:
                ex.close()
                observed = services.resources.get(ex._scheduler.scope_id)
                assert all(a["cleanup"] == "CONFIRMED" for a in observed["allocations"])
                print(json.dumps({"cleanup_confirmed": True, "full_business_run": False}), flush=True)
            raise VerificationOnly()
    except VerificationOnly:
        pass
finally:
    ray.shutdown()
