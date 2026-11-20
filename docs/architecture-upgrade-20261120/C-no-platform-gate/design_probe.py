"""Pure JSON model of original-hash reuse, not a product execution test."""
import copy
import hashlib
import json
from pathlib import Path

def digest(v):
    return hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()

checks=[]
def check(label,value):
    assert value,label
    checks.append(label)

old={'protocol':'demiflow.queue.consumer.v2','platform':'a'*64,'operations':[{'business_code':'c'*64}],'result':'result','settlement':'sink'}
current={**old,'platform':'b'*64}
check('old_vs_current_platform_changes_legacy_hash',digest(old)!=digest(current))
rehydrated={**current,'platform':old['platform']}
check('legacy_reconstruction_preserves_original_hash',digest(rehydrated)==digest(old))
changed=copy.deepcopy(rehydrated);changed['operations'][0]['business_code']='d'*64
check('business_code_still_rejected',digest(changed)!=digest(old))
v3={k:v for k,v in current.items() if k!='platform'};v3['protocol']='demiflow.queue.consumer.v3'
check('new_contract_has_no_platform', 'platform' not in v3)
workspace={'platform':{'digest':'a'*64},'locations':{'out':{'writable':True}},'budgets':{'memory':1024}}
now=copy.deepcopy(workspace);now['platform']['digest']='b'*64
check('legacy_workspace_original_field_reconstruction',digest({**now,'platform':workspace['platform']})==digest(workspace))
now['locations']['out']['writable']=False
check('workspace_permission_change_not_waived',digest({**now,'platform':workspace['platform']})!=digest(workspace))
print(json.dumps({'kind':'pure_json_model_not_product_verification','passed':len(checks),'checks':checks},ensure_ascii=False,indent=2))
