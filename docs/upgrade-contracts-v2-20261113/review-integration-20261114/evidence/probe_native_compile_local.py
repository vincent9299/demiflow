import json,pathlib,tempfile,traceback
import demiflow
root=pathlib.Path.cwd(); assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(root)
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.capabilities import ActionDecision
from demiflow.execution.native_plan import compile_native_topology
results={}
for name in ['linear','union','join']:
 directory=pathlib.Path(tempfile.mkdtemp(prefix='demiflow-native-compile-',dir='/tmp'))
 try:
  with RunContext.start(directory) as run:
   with data.local_execution(workers=1,worker_mode='thread',partitions=1):
    left=data.from_items([{'id':1,'x':1}]);right=data.from_items([{'id':1,'y':2}])
    ds=left if name=='linear' else left.union(left) if name=='union' else left.join(right,on='id')
    with run.action(plan_identity('write_lance',ds._source,ds._plan)) as action:
     ld,pp,ref=compile_native_topology(action,ds._source,ds._plan,decision=ActionDecision('write_lance','datafusion','native_relational','final_commit'))
     action.complete({'probe':'compiled'});results[name]={'status':'compiled','nodes':len(ld['nodes'])}
 except Exception as e:results[name]={'status':'rejected','error':str(e),'trace':traceback.format_exc(limit=7)}
print(json.dumps(results,indent=2))
