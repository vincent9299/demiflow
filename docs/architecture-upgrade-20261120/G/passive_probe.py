"""Inspect real passive model paths; no provider, worker, service or network execution."""
import json
from pathlib import Path
import sys
import threading
import subprocess
import socket
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
assert Path(sys.path[0]).resolve()!=ROOT/'demiflow'
observed={}
for name,callable_ in [('thread_start',threading.Thread.start),('popen',subprocess.Popen),('socket_connect',socket.socket.connect)]:observed[name]=0

def reject(name):
    def called(*args,**kwargs):
        observed[name]+=1
        raise AssertionError('Passive configuration unexpectedly called '+name)
    return called

def modules():
    return sorted(n for n in sys.modules if n.startswith(('demiflow.execution','demiflow.operator_llm','demiflow.services','PIL')))

with patch.object(threading.Thread,'start',reject('thread_start')),patch.object(subprocess,'Popen',reject('popen')),patch.object(socket.socket,'connect',reject('socket_connect')):
    from demiflow.operator_llm import model
    import demiflow
    assert Path(demiflow.__file__).resolve()==ROOT/'demiflow/__init__.py'
    after_import=modules()
    if '--parser-only' in sys.argv:
        from demiflow.operator_llm.parser import parse_prompt_pack
        document={'schema_version':'demiflow_prompt_pack_v2','prompts':{'draw':{'version':'1','model':{'backend':'modelhub','model':'example','api':'images','base_url':'https://example.invalid','api_key_env':''},'template':'Draw','response_format':'image'}}}
        pack=parse_prompt_pack(json.dumps(document))
        assert pack.prompts[0].response_schema['required']==['image']
        assert pack.prompts[0].response_schema['properties']['image']['required']==['uri','sha256']
        assert not any(observed.values())
        print(json.dumps(dict(mode='parser-only',imported_from=str(demiflow.__file__),python=sys.executable,thread_process_network_attempts=observed,added_by_full_image_pack_parser=sorted(set(modules())-set(after_import)),response_schema=pack.prompts[0].response_schema,limit='Current baseline real parser; not post-migration or resource benchmark.'),ensure_ascii=False,indent=2))
        raise SystemExit(0)
    text,diags=model.inspect_prompt_model({'name':'plain','transport':'openai_compatible','base_url':'https://example.invalid','api_key_env':'EXAMPLE_KEY'},label='probe')
    assert text is not None and not diags
    media_input={'backend':'modelhub','model':'example','api':'images','base_url':'https://example.invalid','api_key_env':''}
    image,diags=model.inspect_prompt_model(media_input,label='probe',media=True)
    assert image is not None and not diags and image.provider==media_input
    after_image=modules()
    malformed=dict(media_input,timeout_s=True)
    rejected,diags=model.inspect_prompt_model(malformed,label='probe',media=True)
    assert rejected is None and len(diags)==1 and diags[0].code=='prompt_model_invalid'
    deployed,diags=model.inspect_prompt_model({'name':'plain','deployment':{'profile':'vllm','weights':'/not/read/weights','revision':'fixed','gpus':[0]}},label='probe')
    assert deployed is not None and not diags
    after_deployment=modules()
assert not any(observed.values())
print(json.dumps(dict(imported_from=str(demiflow.__file__),python=sys.executable,python_version=sys.version,thread_process_network_attempts=observed,assertions='plain/image/deployment accepted, bool timeout rejected, model/provider values kept, no runtime acquisition',modules_after_package_import=after_import,added_by_media=sorted(set(after_image)-set(after_import)),added_by_deployment=sorted(set(after_deployment)-set(after_image)),limit='Named operations trapped; not a security proof or physical resource benchmark. Package import may allocate dormant objects without starting threads.'),ensure_ascii=False,indent=2))
