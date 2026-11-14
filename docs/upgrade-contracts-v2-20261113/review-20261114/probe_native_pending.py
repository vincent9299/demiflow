import asyncio,json,tempfile
from pathlib import Path
import demiflow
EXPECTED_ROOT = Path(__file__).resolve().parents[3]
assert Path(demiflow.__file__).resolve().is_relative_to(EXPECTED_ROOT), demiflow.__file__
from types import SimpleNamespace
from demiflow.execution import RunContext
from demiflow.execution.stream_materialize import StreamStageProgress
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.row_scope import ScopedRow,RowScope
class ProbeComplete(Exception): pass
async def main():
 with tempfile.TemporaryDirectory(prefix='demiflow-native-registry-probe-') as tmp:
  with RunContext(Path(tmp)/'run',entry_identity='registry-probe') as run:
   with run.action({'kind':'probe'}) as action:
    p=StreamStageProgress(); p.action=action;p.run=run
    p.stages=[SimpleNamespace(native_snapshot=True,native_pending={
     'worker-a':ScopedRow({'x':1},RowScope('occurrence-a')),
     'worker-b':ScopedRow({'x':2},RowScope('occurrence-b'))})]
    p.buffer_sinks={};p.group_ends=[0];p.has_objects=False
    class EmptySemanticState:
     async def snapshot(self,*args,**kwargs):return {}
    p.state=EmptySemanticState()
    barrier=SimpleNamespace(snapshot_bytes=0,states={})
    # Real align and real BlockSink. Empty semantic-state adapter isolates row-local snapshot.
    await p.align(0,barrier,[{'from_only_first_worker':True}])
    snap=barrier.states['node-0-inflight']
    rows=list(BlockSink(action,'node-0-inflight',staging_only=True).iter_rows(snap['data']))
    result={'occurrences':snap['occurrences'],'rows':rows,'scope':'real align + BlockSink; no crash/resume or scheduling proof'}
    print(json.dumps(result,indent=2))
    assert len(rows)==2 and set(snap['occurrences'])=={'occurrence-a','occurrence-b'}
    # Intentionally abort this isolated action; the probe never publishes a commit.
    raise ProbeComplete()
try:
 asyncio.run(main())
except ProbeComplete:
 pass
