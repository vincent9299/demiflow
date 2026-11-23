from demiflow.execution.contracts import record_codec
import json,statistics,subprocess,time,types,hashlib
from demiflow.execution.exchange import dataflow_bridge as current
from demiflow.execution.contracts.dataflow import Record
baseline=types.ModuleType('demiflow.execution.exchange._codec_baseline')
baseline.__package__='demiflow.execution.exchange'
exec(compile(subprocess.check_output(['git','show','e304640:demiflow/execution/exchange/dataflow_bridge.py'],text=True),'<e304640 codec>','exec'),baseline.__dict__)
rows={'narrow':{'key':7,'left':-0.0,'right':True,'text':'中文é'},
      'wide':{'key':7,'left':-0.0,'right':True,'text':'中x'*50000}}
results=[]
for name,row in rows.items():
 payload=baseline._encode_payload(row,max_bytes=2**20)
 record=Record('a'*64,{'offset':7,'origin':'source'},None,row,len(payload),7)
 doc={'v':3,'sp':record.source_position,'eb':len(payload),'p':json.loads(payload),'r':None}
 raw=baseline._encode_record_document(record,max_bytes=2**20)
 assert record_codec._encode_record_document(record,max_bytes=2**20)==raw
 cases={'wrapper':lambda module:module._record_document_bytes(doc,payload_raw=payload,max_bytes=2**20),
        'encode':lambda module:module._encode_record_document(record,max_bytes=2**20),
        'decode':lambda module:module._decode_record_document(raw,record_id=record.record_id,sequence=record.sequence,max_bytes=2**20)}
 for label,fn in cases.items():
  n=10000 if name=='narrow' else 100
  timings={'baseline':[],'candidate':[]}
  for repeat in range(5):
   for key,module in [('baseline',baseline),('candidate',current)][::1 if repeat%2==0 else -1]:
    start=time.perf_counter()
    for _ in range(n):fn(module)
    timings[key].append(time.perf_counter()-start)
  old,new=(statistics.median(timings[k]) for k in ('baseline','candidate'))
  results.append({'case':name+'_'+label,'iterations':n,'baseline_s':old,'candidate_s':new,'speedup':old/new,'samples':timings,'wire_sha256':hashlib.sha256(raw).hexdigest()})
print(json.dumps({'baseline':'e304640','candidate_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'results':results},indent=2))
