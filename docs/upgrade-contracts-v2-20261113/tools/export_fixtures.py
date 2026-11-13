"""Pure schema samples. Does not execute a business plan or start services."""
import json, runpy, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT))
OUT=ROOT/'docs/upgrade-contracts-v2-20261113/fixtures'
from demiflow.execution.plan_interpreter import interpret_physical_plan
from demiflow.execution.recovery_contract import identity
from demiflow.planning.dag import compile_physical_execution_plan
ns=runpy.run_path(str(ROOT/'tests/test_execution_layout.py'))
for name,fx in [('chain-v1',ns['chain_fixture']()),('queue-v1',ns['chain_fixture'](slice_name='queue_transform_v1')),('fanout-v1',ns['fanout_fixture']())]:
    layout=interpret_physical_plan(fx.ld,fx.pp,bindings=fx.bundle,manifest_ref=fx.mref)
    sample={'fixture_kind':'contract_only_dummy_program_refs','logical_dag':fx.ld,'physical_plan':fx.pp,'manifest_ref':fx.mref,'bindings':fx.bundle}
    (OUT/(name+'.json')).write_text(json.dumps(sample,ensure_ascii=False,indent=2)+'\n')
    print(name,layout.supported_slice,'real interpreter accepted')
# A proposed v2 driver fixture: real DAG/physical compiler, design-only v2 bindings.
fx=ns['chain_fixture']()
pp=compile_physical_execution_plan(logical_dag=fx.ld,plan_revision=0,artifact_manifest_ref=fx.mref,worker_groups=[],recovery=dict(ns['RECOVERY']),feasibility_snapshot_ref=identity(['test-native-driver']),read_domain='test-domain',durability_declared='durable',partition_count=1,scheme_kind='single')
program=fx.doc['programs'][0]
binding={k:v for k,v in fx.doc.items() if k not in ('programs','driver_stage_ids')}
binding.update(schema='demiflow.execution_bindings.v2',plan_id=pp['plan_id'],supported_slice='native_driver_v1',driver_stage_ids=[s['stage_id'] for s in pp['stages']],executions=[dict(execution_id=identity([fx.action_id,'driver','native_relational']),placement='driver',group_id=None,backend='datafusion',stage_ids=program['stage_ids'],profile_ref=next(iter(fx.doc['profiles'])),programs=[dict(program_id=identity([fx.action_id,[]]),input_path=[],plan_ref=program['plan_ref'],source_identity_ref=fx.ld['sources'][0]['source_identity_ref'],sealed_operators=program['sealed_operators'],operator_bindings=program['operator_bindings'])],recovery='final_commit')])
sample={'fixture_kind':'proposed_v2_not_runtime_verified','logical_dag':fx.ld,'physical_plan':pp,'manifest_ref':fx.mref,'bindings':ns['bundle_for'](binding)}
(OUT/'native-driver-v2.json').write_text(json.dumps(sample,ensure_ascii=False,indent=2)+'\n')
print('native-driver-v2: real physical compiler accepted empty worker groups; v2 interpreter not implemented')
