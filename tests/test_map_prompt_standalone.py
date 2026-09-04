"""demiflow map_prompt standalone 冒烟：脱离 Candidate 机制端到端验证。

本地 HTTP mock 充当 OpenAI 兼容端点，验证 parse_prompt_pack →
LocalDatasetExecutor(prompt_packs=...) → map_prompt → schema 校验输出
全链路——二期富化管线直接按此形态使用。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

PACK_YAML = """
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: enrich-v1
    model:
      name: mock-model
      transport: openai_compatible
      base_url_env: MOCK_LLM_BASE_URL
      api_key_env: MOCK_LLM_API_KEY
    schema_retries: 1
    response_schema:
      type: object
      additionalProperties: false
      required: [summary]
      properties:
        summary: {type: string}
    template: |
      Describe the following entity:
      {{ entity | json }}
"""


def test_map_prompt_standalone(monkeypatch):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            user = body["messages"][-1]["content"]
            content = json.dumps({"summary": f"ok:{len(str(user))}"})
            data = json.dumps(
                {"choices": [{"message": {"content": content}}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        monkeypatch.setenv("MOCK_LLM_BASE_URL",
                           f"http://127.0.0.1:{srv.server_port}/v1")
        monkeypatch.setenv("MOCK_LLM_API_KEY", "test-key")

        from demiflow.operator_llm.parser import parse_prompt_pack
        from demiflow.standalone import local_data

        pack = parse_prompt_pack(PACK_YAML)
        ctx = local_data(prompt_packs={"enrich.yaml": pack})
        rows = (ctx.from_items([{"entity": {"name": f"e{i}", "kind": "测试"}}
                                for i in range(3)])
                .map_prompt("enrich", config="enrich.yaml",
                            inputs=["entity"], outputs={"summary": "summary"})
                .take_all())
        assert len(rows) == 3
        assert all(r["summary"].startswith("ok:") for r in rows), rows
        assert all("entity" in r for r in rows)          # 未映射字段保留
        print("[PASS] map_prompt standalone：pack 解析→执行→schema 校验输出")
    finally:
        srv.shutdown()
