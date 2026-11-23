"""Native object/row commit faults; stdout is observation, never recovery state."""
import json
import os
from pathlib import Path
import sys

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef, SourcePosition
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.outputs.object_sink import ObjectRowsSink, ObjectSink
import demiflow.execution.commit as protocol
import demiflow.execution.outputs.object_sink as objects

root,window=Path(sys.argv[1]),sys.argv[2]
prior=RunJournal(root/'run').recovery_identity()
scope=(RunContext.resume(RunRef(str(root/'run'),prior['run_id'])) if prior else
       RunContext.start(root/'run',limits={'max_output_bytes':65536,'max_scratch_bytes':131072,'timeout_s':180}))
if window in {'objects_committed_before_rows','rows_committed_before_return'}:
    objects.fault_point=lambda name,commit_id: os._exit(73) if name==window else None
elif window=='object_publication_before_return':
    original=ObjectSink.commit
    def publish(*args,**kwargs):
        original(*args,**kwargs)
        os._exit(73)
    ObjectSink.commit=publish
elif window!='resume':
    target={'after_prepared':'after_prepared','after_outer_receipt':'after_sink_receipt','after_acked':'after_acked'}[window]
    protocol.fault_point=lambda name,commit_id: os._exit(73) if name==target else None

with scope as run:
    with run.action({'kind':'materialize','fixture':'object-rows-v1'}) as action:
        sink=ObjectRowsSink(action,max_object_bytes=4096)
        coordinator=CommitCoordinator(action)
        facts=coordinator.recover({'output':sink})
        if not [f for f in facts if f['status']!='pending']:
            def rows():
                print('PRODUCER',flush=True)
                run.reserve('requests')
                yield {'id':1,'image':sink.objects.stage((root/'fixture.png').read_bytes())}
            fact=coordinator.commit(1,outputs={'output':(sink,rows())},position=SourcePosition('fixed-fixture',1))
        else:
            fact=facts[0]
        result=list(sink.rows.iter_rows(fact['receipts']['output']['data']['rows']))
        action.complete({'rows':len(result)})
        (root/'observed_rows.json').write_text(json.dumps(result))
