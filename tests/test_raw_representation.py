"""Wire representation bytes and authorization at the actual HTTP boundary."""
from contextlib import contextmanager
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import sqlite3
import threading
from urllib.parse import urlsplit

import pytest
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError
from test_raw_fetch import origin_server


def test_host_rejection_does_not_charge_an_http_request_that_cannot_start(tmp_path):
    with origin_server() as (url,calls):
        with pytest.raises(RecoveryContractError,match='host resource'):
            with RunContext.start(tmp_path/'run'):
                data.from_items([{'request':{'uri':uri}} for uri in (url+'/file',url.replace('127.0.0.1','localhost')+'/file')]).fetch(
                    store=LocalObjectStore(tmp_path/'objects'),concurrency=1,
                    policy=FetchPolicy(max_object_bytes=128*1024,max_hosts=1,http=HttpSource(allow_private=True))).materialize()
        assert calls['/file']==1
        with sqlite3.connect(tmp_path/'run/run.sqlite') as db:
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()==(1,)
            assert sorted(row[0] for row in db.execute('SELECT requests FROM runtime_raw_attempts'))==[0,1]


def test_server_content_encoding_does_not_decode_the_raw_representation(tmp_path):
    encoded=gzip.compress(b'original representation'*1000,mtime=0);headers=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            headers.append(self.headers['Accept-Encoding'])
            self.send_response(200);self.send_header('Content-Length',str(len(encoded)))
            self.send_header('Content-Encoding','gzip');self.end_headers();self.wfile.write(encoded)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever);thread.start()
    try:
        row,=data.from_items([{'request':{'uri':'http://127.0.0.1:'+str(server.server_port),
            'expected_size':len(encoded),'expected_sha256':hashlib.sha256(encoded).hexdigest()}}]).fetch(
                store=LocalObjectStore(tmp_path/'objects'),concurrency=1,
                policy=FetchPolicy(max_object_bytes=65536,http=HttpSource(allow_private=True))).take_all()
        result=row['result']
        assert headers==['identity'] and result['status']=='ok' and result['size_bytes']==len(encoded)
        assert result['source']['content_encoding']=='gzip'
        assert Path(urlsplit(result['raw_ref']['uri']).path).read_bytes()==encoded
    finally:server.shutdown();server.server_close();thread.join(2)
