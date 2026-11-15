from pathlib import Path
import os, sys, subprocess, json, hashlib, time
ROOT=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115')
OUT=Path('/tmp/demiflow-gpt-runtime-v3-review-20261115')
sys.path.insert(0,str(ROOT))
import demiflow
assert Path(demiflow.__file__).resolve().is_relative_to(ROOT)
def inventory():
    names=subprocess.check_output(['git','ls-files','-co','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
    return {n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in sorted(set(names)) if n and (ROOT/n).is_file() and (n.startswith('demiflow/') or n.startswith('tests/'))}
before=inventory()
name=sys.argv[1]
args=sys.argv[2:]
cmd=[sys.executable,'-B','-m','pytest',*args,'-q','-p','no:cacheprovider','--tb=short',f'--basetemp=/tmp/demiflow-gpt-{name}',f'--junitxml={OUT/name}.xml']
env=dict(os.environ,PYTHONPATH=str(ROOT)+os.pathsep+str(OUT),PYTHONDONTWRITEBYTECODE='1')
start=time.time()
with (OUT/(name+'.log')).open('w') as log:
    try: result=subprocess.run(cmd,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600); code=result.returncode
    except subprocess.TimeoutExpired: code='timeout'
after=inventory()
evidence={'command':cmd,'cwd':str(ROOT),'PYTHONPATH':env['PYTHONPATH'],'module':demiflow.__file__,'base':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'exit_code':code,'elapsed_s':time.time()-start,'inputs':before,'source_stable':before==after,'changed_during_run':[n for n in sorted(set(before)|set(after)) if before.get(n)!=after.get(n)]}
(OUT/(name+'.json')).write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({k:v for k,v in evidence.items() if k!='inputs'},ensure_ascii=False),flush=True)
