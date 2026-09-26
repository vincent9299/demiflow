"""Plain-text prompt responses keep the same journal and checkpoint guarantees."""
import json,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.errors import PromptPackError

PACK='''
schema_version: demiflow_prompt_pack_v2
prompts:
  write:
    version: v1
    response_format: text
    model:
      name: mock
      transport: openai_compatible
      base_url_env: TEXT_TEST_URL
      api_key_env: TEXT_TEST_KEY
    response_schema:
      type: object
      required: [result]
      additionalProperties: false
      properties:
        result: {type: string, minLength: 1}
    template: '{{ payload }}'
'''

@pytest.mark.parametrize('thinking',[False,True])
def test_plain_text_http_and_journal_replay(tmp_path,monkeypatch,thinking):
    bodies=[];article='## 标题\n正文“引号”无需JSON转义。\n【完成】'
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            result={'choices':[{'message':{'content':('<think>private computation</think>\n' if thinking else '')+article},'finish_reason':'stop'}]}
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(result).encode())
        def log_message(self,*args):pass
    srv=ThreadingHTTPServer(('127.0.0.1',0),Handler);worker=threading.Thread(target=srv.serve_forever,daemon=True);worker.start()
    monkeypatch.setenv('TEXT_TEST_URL',f'http://127.0.0.1:{srv.server_port}/v1');monkeypatch.setenv('TEXT_TEST_KEY','local')
    try:
        for i in range(2):
            ctx=DataAPI()
            options={'journal_dir':str(tmp_path/'calls'),'request_options':{'response_format':{'type':'json_object'},'chat_template_kwargs':{'enable_thinking':thinking}},'require_finish_reason_stop':True}
            rows=(ctx.from_items([{'input':'写文章'}]).map_prompt_async('write',config=parse_prompt_pack(PACK),options=options,inputs={'payload':'input'},output='article').checkpoint(tmp_path/f'out{i}.jsonl',version='v1').take_all())
            assert rows[0]['article']==article
        assert len(bodies)==1
        assert 'response_format' not in bodies[0]
        assert 'plain text' in bodies[0]['messages'][0]['content']
    finally:srv.shutdown();srv.server_close();worker.join()

def test_text_contract_rejects_ambiguous_output_shape():
    with pytest.raises(PromptPackError):parse_prompt_pack(PACK.replace('result: {type: string, minLength: 1}','result: {type: array, items: {}}'))
