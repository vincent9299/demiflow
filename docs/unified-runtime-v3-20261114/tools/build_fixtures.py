from pathlib import Path
import copy,hashlib,json
O=Path(__file__).resolve().parents[1];F=O/'fixtures'
h=lambda x:hashlib.sha256(x.encode()).hexdigest()
ref=dict(digest=h('fixture bytes'),size_bytes=13,store_id='test-store',key='objects/'+h('fixture bytes'),media_type='application/vnd.demiflow.record.v3')
cursor=dict(split_id=h('split'),offset={'next':7},next_sequence=7,end_of_input=False,lease_ref=None,pending_refs=[ref])
record=dict(record_id=h('row'),source_position={'next':6},payload_ref=ref,inline=None,encoded_bytes=13,sequence=6)
event=dict(schema='demiflow.dataflow_event.v3',kind='reserve',operation_id=h('reserve'),run_id='r-fixture',attempt=1,fence='f-fixture',allocation_id='a-fixture',boot_id='b-fixture',plan_revision=0,scope_id='s-fixture',controller_epoch=1,payload_ref=None,payload={'operation_id':h('reserve'),'amounts':{'row_bytes':13}})
cases=[('source-cursor','SourceCursor',cursor,True),('source-pending','SourcePoll',dict(status='pending',record=None,retry_after_ms=10),True),('source-eoi','SourcePoll',dict(status='eoi',record=None,retry_after_ms=0),True),('source-record','SourcePoll',dict(status='record',record=record,retry_after_ms=0),True),('reserve-inline','WorkerEvent',event,True),('reply-error','ControlReply',dict(operation_id=h('reserve'),status='error',payload=None,payload_ref=None,error_code='BUDGET_EXCEEDED'),True),('finite-scope','FiniteScope',dict(scope_id=h('scope'),generation=0,membership=['stage0:part0','stage0:part1'],completion_condition='input_eoi'),True)]
x=copy.deepcopy(event);x['payload_ref']=ref;cases.append(('invalid-event-double-payload','WorkerEvent',x,False))
x=copy.deepcopy(cursor);x['next_sequence']=-1;cases.append(('invalid-cursor','SourceCursor',x,False))
x=dict(status='pending',record=None,retry_after_ms=0);cases.append(('invalid-pending','SourcePoll',x,False))
x=copy.deepcopy(record);x['inline']={};cases.append(('invalid-record-double-payload','Record',x,False))
x=copy.deepcopy(event);x['kind']='execute_sql';cases.append(('invalid-authority-command','WorkerEvent',x,False))
index=[]
for name,kind,value,valid in cases:
 file=name+'.json';doc={'schema':'demiflow.runtime_wire.v3','type':kind,'payload':value};(F/file).write_text(json.dumps(doc,ensure_ascii=False,indent=2)+'\n');index.append({'file':file,'type':kind,'expected_valid':valid,'purpose':'wire contract example, no real engine/artifact acceptance'})
(F/'index.json').write_text(json.dumps(index,indent=2)+'\n')
