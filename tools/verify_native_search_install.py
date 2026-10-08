"""Install the wheel outside the checkout and exercise only its Dataset API."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('wheel')
    parser.add_argument('--receipt',required=True)
    parser.add_argument('--live-proxy-env')
    args=parser.parse_args()
    wheel=Path(args.wheel).resolve()
    with zipfile.ZipFile(wheel) as z:
        names=z.namelist()
        adapters=[n for n in names if '/searx/engines/' in n and n.endswith('.py') and not n.endswith('/__init__.py')]
        assert len(adapters)==257, len(adapters)
        for name in ('LICENSE','AUTHORS.rst','BASELINE.json','baseline.tar.gz','LOCAL_CHANGES.md'):
            assert 'demiflow/_vendor/searxng/'+name in names
    with tempfile.TemporaryDirectory(prefix='demiflow-native-install-') as tmp:
        root=Path(tmp); target=root/'installed'
        subprocess.run([sys.executable,'-m','pip','install','--no-deps','--no-compile','--target',str(target),str(wheel)],
            cwd=root,stdout=subprocess.DEVNULL,check=True)
        program='''
import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from demiflow import data
from demiflow.collect import SearchConfig,Secret,search_source_inventory
from demiflow.collect.session import WebSession
from demiflow.collect.native_search.config import baseline_id, runtime_id
import demiflow
assert Path(demiflow.__file__).is_relative_to(Path(sys.argv[1]))
web=WebSession(cache_path='cache.sqlite',object_directory='objects',search=SearchConfig(
 engines=({'name':'demo','engine':'demo_offline'},),language='en',workers=1))
rows=[]
stats=(data.from_items([{'requests':[{'request_id':'installed','query':'native wheel','bindings':[]}]}])
 .search_web(requests='requests',output='found',session=web)
 .map(lambda row:rows.append(row) or row).run_stream())
assert rows[0]['found'][0]['status']=='ok',rows
assert 'searx' not in sys.modules
for name,mod in list(sys.modules.items()):
 if name.startswith('demiflow') and getattr(mod,'__file__',None):
  assert Path(mod.__file__).is_relative_to(Path(sys.argv[1])),(name,mod.__file__)
inv=search_source_inventory()
metrics=stats.metrics['resources']['WebSession:0']['native_search']
assert metrics['http_requests']==0
assert metrics['worker_starts']==1
receipt={'status':'passed','baseline_adapters':len(inv['modules'])-1,
 'baseline_id':baseline_id(),'runtime_id':runtime_id(),
 'named_sources':len(inv['sources']),'platform_sources':len(inv['platform_sources']),
 'metrics':metrics,'candidate_count':len(rows[0]['found'][0]['candidates']),
 'imports_from_installed_wheel':True,'vendor_absent_in_pipeline_interpreter':True}
if len(sys.argv)>2 and sys.argv[2]:
 live=WebSession(cache_path='live.sqlite',object_directory='live-objects',search=SearchConfig(
  engines=('wikisearch','wikipedia'),language='en',workers=2,request_concurrency=2,
  host_concurrency=1,host_interval_s=.5,retries=0,timeout_s=25,proxy=Secret(sys.argv[2])))
 live_rows=[]
 live_stats=(data.from_items([{'requests':[{'request_id':'installed-live','query':'Ginkgo biloba','bindings':[]}]}])
  .search_web(requests='requests',output='found',session=live)
  .map(lambda row:live_rows.append(row) or row).run_stream())
 receipt['live']={'rows':live_rows,'metrics':live_stats.metrics}
print(json.dumps(receipt))
'''
        env=dict(os.environ)
        env.pop('PYTHONPATH',None)
        result=subprocess.run([sys.executable,'-I','-c',program,str(target),args.live_proxy_env or ''],cwd=root,env=env,
                              capture_output=True,text=True,timeout=60)
        if result.returncode:
            raise RuntimeError(result.stderr)
        receipt=json.loads(result.stdout)
        receipt['wheel']=wheel.name
        receipt['wheel_sha256']=hashlib.sha256(wheel.read_bytes()).hexdigest()
        Path(args.receipt).write_text(json.dumps(receipt,indent=2)+'\n')
        if 'live' in receipt:
            result=receipt['live']['rows'][0]['found'][0]
            receipt={**receipt,'live':{'status':result['status'],'sources':{r['engine']:r['status'] for r in result['engine_receipts']}}}
        print(json.dumps(receipt,indent=2))


if __name__=='__main__':
    main()
