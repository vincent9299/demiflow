"""Save exact design inputs and honest per-round findings; no automatic review claims."""
import hashlib,json,sys,datetime
from pathlib import Path
O=Path(__file__).resolve().parents[1]
mode,n=sys.argv[1],int(sys.argv[2]);p=O/'reviews'/f'round-{n:02d}.json'
manifest=json.loads((O/'plan.json').read_text())
inputs={str(f.relative_to(O)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(O.rglob('*')) if f.is_file() and not any(k in f.parts for k in ['reviews','__pycache__']) and f.suffix in {'.md','.json','.pyi','.py'}}
snapshot={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'inputs':inputs,'task_ids':[t['id'] for t in manifest['tasks']],'api':[a['symbol'] for a in manifest['api']]}
if mode=='begin':
 if p.exists():raise SystemExit('round exists; never overwrite prior review')
 d={'round':n,'scope':'whole design package: all leaves, APIs, contracts, ownership, roots and end-to-end seams','method':'source-grounded reasoning plus whole-manifest/card structural traversal; not runtime verification or an independent reviewer','before':snapshot,'findings':[], 'status':'in_progress'}
else:
 d=json.loads(p.read_text());d['after']=snapshot;d['findings']=json.loads(sys.argv[3]);d['structural_result']=json.loads((O/'evidence/design-check.json').read_text());d['status']='completed_with_remaining_issues' if d['structural_result']['errors'] else 'completed_design_checked';d['runtime_tests_run']=False
p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');print(n,mode,len(snapshot['task_ids']),len(inputs))
