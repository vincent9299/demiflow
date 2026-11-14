import json,tempfile
from pathlib import Path
import demiflow
EXPECTED_ROOT = Path(__file__).resolve().parents[3]
assert Path(demiflow.__file__).resolve().is_relative_to(EXPECTED_ROOT), demiflow.__file__
from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.run_context import current_action,current_run
from demiflow.execution.executors.local import LocalDatasetExecutor
observed={};original={n:getattr(LocalDatasetExecutor,n) for n in ['take','iter_rows','count']}
def snap(name):
 a=current_action();observed[name]={'action_present':a is not None,'run_present':current_run() is not None,'action_id':getattr(a,'action_id',None)}
def take(self,*a,**k):snap('take');return []
def rows(self,*a,**k):snap('iter_rows');return iter(())
def count(self,*a,**k):snap('count');return 0
try:
 LocalDatasetExecutor.take=take;LocalDatasetExecutor.iter_rows=rows;LocalDatasetExecutor.count=count
 with tempfile.TemporaryDirectory(prefix='demiflow-scope-probe-') as tmp:
  ds=data.from_items([{'x':1}])
  with RunContext(Path(tmp)/'run',entry_identity='scope-probe'):
   ds.take(1);list(ds.iter_rows());ds.count()
finally:
 for n,f in original.items():setattr(LocalDatasetExecutor,n,f)
print(json.dumps(observed,indent=2))
assert observed['count']['action_present'] and not observed['take']['action_present'] and not observed['iter_rows']['action_present']
