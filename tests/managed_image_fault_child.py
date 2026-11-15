import os
from pathlib import Path
import sys

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.object_sink import ObjectSink
import demiflow.execution.commit as protocol
import demiflow.execution.object_sink as objects

root,window=Path(sys.argv[1]),sys.argv[2]
prior=RunJournal(root/'run').recovery_identity()
scope=RunContext.resume(RunRef(str(root/'run'),prior['run_id'])) if prior else RunContext.start(root/'run',
    limits={'memory_bytes':160*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144,
            'max_output_bytes':2*1024**2,'max_scratch_bytes':8*1024**2,'timeout_s':180})
if window=='after_provider_before_artifact':
    ObjectSink.stage=lambda *args,**kwargs:os._exit(73)
elif window=='after_staging_before_prepared':
    def kill(name,commit_id):
        fact=RunJournal(root/'run').execution_commit(commit_id)
        if name=='after_prepare_before_record' and fact['intent']['sinks']==['materialize']:os._exit(73)
    protocol.fault_point=kill
elif window=='objects_committed_before_rows':
    objects.fault_point=lambda name,commit_id:os._exit(73) if name==window else None
elif window=='object_publication_before_return':
    original=ObjectSink.commit
    def kill(*args,**kwargs):
        original(*args,**kwargs);os._exit(73)
    ObjectSink.commit=kill
elif window=='after_acked':
    def kill(name,commit_id):
        fact=RunJournal(root/'run').execution_commit(commit_id)
        if name=='after_acked' and fact['intent']['sinks']==['materialize']:os._exit(73)
    protocol.fault_point=kill
with scope:
    result=data.from_items([{'instruction':'paint'}]).map_prompt_async('generate',config=str(root/'pack.json'),
        inputs={'instruction':'instruction'},output='image',call_output='call',error_output='error',max_requests=2,
        options={'object_store':str(root/'assets'),'image_encoding':'preserve','io_workers':1,'prepare_workers':1,
            'response_workers':1,'limits':{'max_images':2,'max_image_bytes':4096,'max_text_bytes':1024,
                'max_pixels':64,'max_total_pixels':128,'max_request_bytes':8192,'max_response_bytes':8192}})
    result=result.read_object(reference='image',output='pixels',root=str(root/'assets'),layout='sha256_prefix2',max_bytes=4096)
    result.materialize().select_columns(['image','pixels']).write_lance(str(root/'out.lance'),mode='create')
