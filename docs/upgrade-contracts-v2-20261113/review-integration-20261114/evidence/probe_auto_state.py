from pathlib import Path
import demiflow,json,tempfile
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
from demiflow import data
from demiflow.execution.operator_policy import classify_operator_state
from demiflow.planning.traits import unified_operation_traits
from demiflow.execution.run_context import RunContext
class Counter:
 def __init__(self):self.count=0
 def __call__(self,row):self.count+=1;return {'count':self.count}
counter=Counter();ds=data.from_items([{'id':1},{'id':2}]).map(counter,concurrency=1)
result={'classification':vars(classify_operator_state(counter,phase='instance')),'sealed_ownership':unified_operation_traits(ds._plan.operations[0]).state_ownership}
try:
 with RunContext.start(Path(tempfile.mkdtemp(prefix='demiflow-auto-state-',dir='/tmp'))):
  result['rows']=ds.materialize().take_all()
except Exception as e:result['error']=str(e)
print(json.dumps(result,indent=2))
