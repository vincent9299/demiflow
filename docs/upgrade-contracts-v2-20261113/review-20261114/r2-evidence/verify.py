import hashlib,json,os,subprocess,sys
from pathlib import Path
root=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_contracts_20261113');out=root/'docs/upgrade-contracts-v2-20261113';dest=Path('/tmp/demiflow-review-r2-20261114')
assert Path.cwd()==root
tracked=sorted(p for p in out.rglob('*') if p.is_file() and 'r2-evidence' not in p.parts and '__pycache__' not in p.parts and p.name!='design-validation.json')
source=json.loads((out/'baseline.json').read_text())['files']
paths=tracked+[root/p for p in source]
manifest={'schema':'demiflow.design_checks.v1','cwd':str(root),'python':sys.executable,'python_version':sys.version,'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'git_status':subprocess.check_output(['git','status','--porcelain'],text=True),'inputs':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},'commands':[],'runtime_business_tests_run':False}
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=str(root))
for script in ['test_gate_evidence.py','test_gate_aggregation.py','test_scope_guard.py','check_design.py']:
 command=[sys.executable,str(out/'tools'/script)]
 proc=subprocess.run(command,cwd=root,env=env,capture_output=True,text=True,timeout=60)
 name=script.removesuffix('.py')+'.log';(dest/name).write_text(proc.stdout+proc.stderr)
 manifest['commands'].append({'argv':command,'cwd':str(root),'environment':{'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(root)},'exit_code':proc.returncode,'log':name,'log_sha256':hashlib.sha256((dest/name).read_bytes()).hexdigest()})
 print(script,proc.returncode)
 if proc.returncode: print((proc.stdout+proc.stderr)[-2500:])
manifest['inputs_unchanged_after_checks']=all(hashlib.sha256((root/p).read_bytes()).hexdigest()==sha for p,sha in manifest['inputs'].items())
(dest/'verification.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
assert manifest['inputs_unchanged_after_checks'] and all(c['exit_code']==0 for c in manifest['commands'])
