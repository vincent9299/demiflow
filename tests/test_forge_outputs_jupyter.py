"""Real private Jupyter MIME fixture; proves protocol/render data, NOT B1.

No business/model code. No shared dependency install. The ordinary temporary
kernel is never used as a production execution fallback or isolation evidence.
"""
import base64
import hashlib
import os
from pathlib import Path
import time
from datetime import datetime
from demiflow.forge.schema import ForgeError

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import output_reader
from demiflow.workbench.analysis.jupyter_protocol import ExecutionOutput
from demiflow.workbench.analysis.output_messages import jupyter_envelope
from test_forge_outputs_media import image_bytes, witness_and_raw


def test_real_private_jupyter_mime_output_projects_to_same_typed_renderer_contract(tmp_path):
    from jupyter_client import KernelManager
    generation='private-fixture-generation'
    png=base64.b64encode(image_bytes()).decode()
    code='''from IPython.display import display, HTML, Markdown, JSON, Image
import base64
print("real kernel stdout")
display(JSON({"zero":0,"false":False,"null":None}))
display(Markdown("# Real Markdown\\n\\nKept as source"))
display(HTML("<h3>Real HTML</h3><script>parent.pwned=true</script>"))
display(Image(data=base64.b64decode("'''+png+'''")))
'''
    manager=KernelManager(connection_file=str(tmp_path/'connection.json'),transport='ipc',ip=str(tmp_path/'ipc'))
    client=None
    env={'PATH':str(Path(os.sys.executable).parent)+':/usr/bin:/bin','HOME':str(tmp_path),
        'IPYTHONDIR':str(tmp_path/'ipython'),'JUPYTER_CONFIG_DIR':str(tmp_path/'jupyter'),
        'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1','MKL_NUM_THREADS':'1','PYTHONNOUSERSITE':'1'}
    try:
        manager.start_kernel(cwd=str(tmp_path),env=env)
        client=manager.client();client.start_channels();client.wait_for_ready(timeout=15)
        parent=client.execute(code,allow_stdin=False,store_history=False,stop_on_error=True)
        state=ExecutionOutput(generation=generation,msg_id=parent)
        deadline=time.monotonic()+15
        original_failure=None
        while not state.complete and time.monotonic()<deadline:
            for channel,method in [('iopub',client.get_iopub_msg),('shell',client.get_shell_msg)]:
                try:message=method(timeout=.05)
                except Exception as error:
                    from queue import Empty
                    if isinstance(error,Empty):continue
                    raise
                if original_failure is None and type(message.get('header',{}).get('date')) is datetime:
                    try:
                        c.encoded_size(message,maximum=16*1024*1024,nodes=32768,depth=16)
                    except ForgeError as error:
                        original_failure={'fixture':'real_private_jupyter_not_B1','condition':'unmodified JSON counter on native jupyter_client message',
                            'error_type':type(error).__name__,'error_code':error.code,'error':str(error),
                            'header_date_type':'datetime.datetime','header_date':message['header']['date'].isoformat(),
                            'message_type':message['header']['msg_type'],'parent_msg_id':message['parent_header'].get('msg_id')}
                state.consume(channel,jupyter_envelope(message),generation=generation)
        assert state.complete and state.status=='succeeded'
        assert original_failure and original_failure['error_code']=='invalid_request'
        witness,_=witness_and_raw(state.outputs)
        witness['provenance'].update(kernel_generation=generation,source_sha256=hashlib.sha256(code.encode()).hexdigest())
        value={**state.snapshot(),'execution_id':'execution','cell_id':'cell','source_sha256':witness['provenance']['source_sha256'],
            'language':'python','session_id':'session','final_status':'succeeded','raw_fd_output_collected':False}
        raw=c.encoded(value,maximum=16*1024*1024,nodes=32768)
        witness['artifact'].update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
        page=output_reader.project_document(raw,witness,limit=16)
        formats={rep['format'] for item in page['items'] for rep in item['representations'] if rep['available']}
        assert {'json','md','html','image','text'}<=formats
        json_output=next(rep for item in page['items'] for rep in item['representations'] if rep['mime']=='application/json')
        assert json_output['value']=={'zero':0,'false':False,'null':None}
        html_output=next(rep for item in page['items'] for rep in item['representations'] if rep['mime']=='text/html')
        assert '<h3>Real HTML</h3>' in html_output['preview']['preview'] and 'parent.pwned' not in html_output['preview']['preview']
        evidence=os.environ.get('FORGE_OUTPUT_EVIDENCE')
        if evidence:
            path=Path(evidence);path.mkdir(parents=True,exist_ok=True)
            (path/'real-jupyter-before-normalization.json').write_bytes(c.encoded(original_failure))
            (path/'private-jupyter-output_v1.json').write_bytes(c.encoded(page))
            (path/'private-jupyter-witness.json').write_bytes(c.encoded({'kind':'private_protocol_fixture_not_B1','provenance':witness['provenance'],'formats':sorted(formats),'raw_bytes':len(raw),'output_objects':len(state.outputs)}))
    finally:
        if client is not None:client.stop_channels()
        manager.shutdown_kernel(now=True)
        assert not manager.is_alive()
