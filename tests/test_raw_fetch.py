"""Public Dataset.fetch against real local HTTP sockets and native commits."""
from collections import Counter
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
import threading

import pytest

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import RecoveryContractError, UnresolvedCommit

BODY = bytes(range(256))*257


@contextmanager
def origin_server():
    calls = Counter()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self,*args):
            pass
        def do_GET(self):
            calls[self.path] += 1
            if self.path == '/redirect':
                self.send_response(302)
                self.send_header('Location','/signed/opaque-secret?unusual=private-locator')
                self.send_header('Content-Length','0')
                self.end_headers()
                return
            status = {'/missing':404,'/denied':403,'/partial':206,'/retry':503}.get(self.path,200)
            body = b'' if self.path == '/empty' or status != 200 else BODY
            self.send_response(status)
            self.send_header('ETag','"fixed-v1"')
            if self.path != '/unframed':
                self.send_header('Content-Length',str(len(body)))
            else:
                self.send_header('Connection','close')
                self.close_connection=True
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError,ConnectionResetError):
                pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever,daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}',calls
    finally:
        server.shutdown();server.server_close();thread.join()


def policy(**kwargs):
    return FetchPolicy(max_object_bytes=128*1024,max_attempts=2,backoff_initial_s=.001,
                       http=HttpSource(allow_private=True),**kwargs)


def fetch(root, requests, p=None, concurrency=1, durable=False):
    rows=[{'key':n,'request':r} for n,r in enumerate(requests)]
    result=data.from_items(rows).fetch(store=LocalObjectStore(root),policy=p or policy(),concurrency=concurrency)
    if durable:result=result.materialize()
    return result.take_all()


def test_raw_preserves_row_and_commits_verified_object(tmp_path):
    with origin_server() as (url,calls), RunContext.start(tmp_path/'run') as run:
        request={'uri':url+'/file','expected_size':len(BODY),'expected_sha256':hashlib.sha256(BODY).hexdigest(),
                 'source_version':{'kind':'http_etag','value':'"fixed-v1"'}}
        rows=fetch(tmp_path/'objects',[request])
        receipt=rows[0]['result']
        assert rows[0]['request']==request and rows[0]['key']==0
        assert receipt['status']=='ok' and receipt['terminal'] is True
        assert receipt['checks']['size_verified'] and receipt['checks']['sha256_verified']
        assert ObjectRef(**receipt['raw_ref']).read(max_bytes=len(BODY))==BODY
        assert receipt['attempts']==receipt['http_requests']==1
        assert calls['/file']==1
        assert not list((tmp_path/'objects').rglob('.pending/*'))
        with sqlite3.connect(run.directory/'run.sqlite') as db:
            assert db.execute('SELECT count(*) FROM runtime_raw_attempts').fetchone()[0]==1
            grants,wire,used,done=db.execute('SELECT grant_no,granted,used,finalized FROM runtime_raw_attempts').fetchone()
            assert grants<=2 and used==len(BODY) and done==1
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='object_bytes'").fetchone()[0]==len(BODY)
            assert db.execute("SELECT count(*) FROM runtime_commits WHERE status!='acked'").fetchone()[0]==0


@pytest.mark.parametrize('path,reason,attempts',[
    ('/missing','not_found',1),('/denied','access_denied',1),('/partial','unsupported_response',1),('/retry','remote_transient',2)])
def test_known_remote_failures_are_rows(tmp_path,path,reason,attempts):
    with origin_server() as (url,calls):
        rows=fetch(tmp_path/'objects',[{'uri':url+path},{'uri':url+'/empty'}])
        receipt=rows[0]['result']
        assert receipt['reason']==reason and receipt['raw_ref'] is None
        assert receipt['attempts']==attempts and calls[path]==attempts
        assert receipt['status']==('retryable' if path=='/retry' else 'failed')
        assert rows[1]['result']['status']=='ok' and rows[1]['result']['size_bytes']==0


@pytest.mark.parametrize('size',[0,1,20000])
def test_expected_size_is_write_ceiling_without_content_length(tmp_path,size,monkeypatch):
    from demiflow.execution.raw_objects import RawWriter
    sizes=[]
    original=RawWriter.write
    def observe(self,chunk):
        sizes.append(self.size+len(chunk))
        return original(self,chunk)
    monkeypatch.setattr(RawWriter,'write',observe)
    with origin_server() as (url,calls):
        receipt=fetch(tmp_path/'objects',[{'uri':url+'/unframed','expected_size':size}])[0]['result']
        assert receipt['reason']=='size_mismatch' and receipt['raw_ref'] is None
        assert all(n<=size for n in sizes)
        assert not list((tmp_path/'objects').rglob('.pending/*'))


def test_expected_oversize_rejects_before_network(tmp_path):
    with origin_server() as (url,calls):
        receipt=fetch(tmp_path/'objects',[{'uri':url+'/file','expected_size':2**30}])[0]['result']
        assert receipt['reason']=='object_too_large' and not calls


@pytest.mark.parametrize('extra,reason',[
    ({'expected_sha256':'0'*64},'sha256_mismatch'),
    ({'source_version':{'kind':'http_etag','value':'"other"'}},'source_version_mismatch'),
    ({'expected_size':3},'size_mismatch')])
def test_constraints_cannot_accept_other_object(tmp_path,extra,reason):
    with origin_server() as (url,calls):
        receipt=fetch(tmp_path/'objects',[{'uri':url+'/file',**extra}])[0]['result']
        assert receipt['reason']==reason and receipt['status']=='failed'


def test_redirect_locator_not_in_rows_or_control_store(tmp_path,caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    with origin_server() as (url,calls),RunContext.start(tmp_path/'run') as run:
        receipt=fetch(tmp_path/'objects',[{'uri':url+'/redirect'}])[0]['result']
        assert receipt['status']=='ok'
        assert receipt['source']['final_uri'] is None and receipt['source']['locator_redacted']
        assert receipt['http_requests']==2
        assert calls['/signed/opaque-secret?unusual=private-locator']==1
    for file in (tmp_path/'run').rglob('*'):
        if file.is_file():
            raw=file.read_bytes()
            assert b'opaque-secret' not in raw and b'private-locator' not in raw
    assert 'opaque-secret' not in json.dumps(receipt)
    assert 'opaque-secret' not in caplog.text and 'private-locator' not in caplog.text


def test_resume_checks_raw_bytes_without_http(tmp_path):
    with origin_server() as (url,calls):
        with pytest.raises(RuntimeError,match='interrupt fixture'):
            with RunContext.start(tmp_path/'run') as run:
                rows=fetch(tmp_path/'objects',[{'uri':url+'/file'}],durable=True)
                reference=run.ref
                raise RuntimeError('interrupt fixture')
        with pytest.raises(RuntimeError,match='interrupt fixture'):
            with RunContext.resume(reference):
                again=fetch(tmp_path/'objects',[{'uri':url+'/file'}],durable=True)
                raise RuntimeError('interrupt fixture')
        assert rows==again and calls['/file']==1
        from urllib.parse import urlsplit
        target=Path(urlsplit(rows[0]['result']['raw_ref']['uri']).path)
        target.write_bytes(b'corrupt')
        with pytest.raises((RecoveryContractError,UnresolvedCommit)):
            with RunContext.resume(reference):
                fetch(tmp_path/'objects',[{'uri':url+'/file'}],durable=True)
        assert calls['/file']==1


def test_concurrent_identical_objects_converge(tmp_path):
    with origin_server() as (url,calls), RunContext.start(tmp_path/'run',limits={'max_row_bytes':256*1024,'max_batch_bytes':1024*1024}):
        rows=fetch(tmp_path/'objects',[{'uri':url+'/file'}]*4,concurrency=4)
        refs=[r['result']['raw_ref'] for r in rows]
        assert refs==[refs[0]]*4
        assert calls['/file']==4


def test_local_configuration_error_is_not_failure_receipt(tmp_path):
    from demiflow.secrets import Secret
    p=FetchPolicy(http=HttpSource(headers={'authorization':Secret('DEMIFLOW_MISSING_TEST_SECRET')},
                                  credential_origin='https://example.org'))
    with pytest.raises(ValueError,match='Missing secret'):
        fetch(tmp_path/'objects',[{'uri':'https://example.org/a'}],p)


def test_declarations_are_lazy_and_legacy_secret_is_same(tmp_path):
    from demiflow.secrets import Secret
    from demiflow.collect.native_search.config import Secret as OldSecret
    assert Secret is OldSecret
    store=LocalObjectStore(tmp_path/'objects')
    data.from_items([]).fetch(store=store)
    assert not store.directory.exists()
    with pytest.raises(ValueError):
        LocalObjectStore(tmp_path/'shared',read_domain='shared')
    with pytest.raises((TypeError,ValueError)):
        FetchPolicy(http={'verify':False})


def test_default_fetch_four_small_objects_needs_no_internal_budget_configuration(tmp_path):
    with origin_server() as (url,calls):
        rows=data.from_items([{'request':{'uri':url+'/file'}} for _ in range(4)]).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(http=HttpSource(allow_private=True))).take_all()
        assert len(rows)==4 and all(row['result']['status']=='ok' for row in rows)
        assert calls['/file']==4


def test_default_fetch_bounded_take_uses_same_payload_admission(tmp_path):
    with origin_server() as (url,calls):
        rows=data.from_items([{'request':{'uri':url+'/file'}} for _ in range(4)]).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=FetchPolicy(http=HttpSource(allow_private=True))).take(4)
        assert len(rows)==4 and all(row['result']['status']=='ok' for row in rows)


def test_secret_header_origin_ownership_survives_redirects(tmp_path,monkeypatch):
    import datetime,ipaddress,ssl
    from cryptography import x509
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import hashes,serialization
    from cryptography.x509.oid import NameOID
    from demiflow.secrets import Secret
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'localhost')])
    now=datetime.datetime.now(datetime.timezone.utc)
    certificate=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
        .not_valid_after(now+datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
        .sign(key,hashes.SHA256()))
    cert=tmp_path/'cert.pem';private=tmp_path/'key.pem'
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    received=[]
    class Target(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            received.append(('target',dict(self.headers)))
            self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'ok')
    target=ThreadingHTTPServer(('127.0.0.1',0),Target)
    tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,private)
    target.socket=tls.wrap_socket(target.socket,server_side=True)
    target_url=f'https://127.0.0.1:{target.server_port}'
    class Origin(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            received.append((self.path,dict(self.headers)))
            if self.path=='/same':destination='/cross'
            else:destination=target_url+'/object'
            self.send_response(302);self.send_header('Location',destination);self.send_header('Content-Length','0');self.end_headers()
    source=ThreadingHTTPServer(('127.0.0.1',0),Origin)
    url=f'http://127.0.0.1:{source.server_port}'
    threads=[threading.Thread(target=s.serve_forever,daemon=True) for s in (source,target)]
    for t in threads:t.start()
    monkeypatch.setenv('DEMIFLOW_RAW_TEST_SECRET','synthetic-only')
    try:
        p=FetchPolicy(max_object_bytes=1024,http=HttpSource(allow_private=True,ca_file=str(cert),
            credential_origin=url,redirect_origins=(target_url,),headers={
                'x-api-key':Secret('DEMIFLOW_RAW_TEST_SECRET'),'authorization':Secret('DEMIFLOW_RAW_TEST_SECRET'),
                'cookie':Secret('DEMIFLOW_RAW_TEST_SECRET'),'x-public':'visible'}))
        result=fetch(tmp_path/'objects',[{'uri':url+'/same'}],p)[0]['result']
        assert result['status']=='ok'
        for location,headers in received:
            lowered={k.lower():v for k,v in headers.items()}
            assert lowered['x-public']=='visible'
            for key in ('x-api-key','authorization','cookie'):
                if location=='target':assert key not in lowered
                else:assert lowered[key]=='synthetic-only'
    finally:
        for s in (source,target):s.shutdown();s.server_close()
        for t in threads:t.join()


@pytest.mark.parametrize('window',['after_acked','settlement_response_lost'])
def test_native_recovery_closes_raw_settlement_once(tmp_path,window,monkeypatch):
    from demiflow.execution import commit
    from demiflow.execution.raw_budget import RawBudget
    original_fault=commit.fault_point
    original_settle=RawBudget.settle_disk
    active=[None];injected=[False]
    def fault(name,commit_id):
        if name=='after_acked' and window=='after_acked' and not injected[0]:
            fact=active[0].journal.execution_commit(commit_id)
            if fact['intent']['sinks']==['raw']:
                injected[0]=True
                raise RuntimeError('raw fault fixture')
    def settle(self,retained,**kwargs):
        original_settle(self,retained,**kwargs)
        if window=='settlement_response_lost' and not injected[0]:
            injected[0]=True
            raise RuntimeError('raw fault fixture')
    monkeypatch.setattr(commit,'fault_point',fault)
    monkeypatch.setattr(RawBudget,'settle_disk',settle)
    with origin_server() as (url,calls):
        with pytest.raises(RuntimeError,match='raw fault fixture'):
            with RunContext.start(tmp_path/'run') as run:
                active[0]=run;ref=run.ref
                fetch(tmp_path/'objects',[{'uri':url+'/file'}],durable=True)
        monkeypatch.setattr(commit,'fault_point',original_fault)
        monkeypatch.setattr(RawBudget,'settle_disk',original_settle)
        for _ in range(2):
            with pytest.raises(RuntimeError,match='repeat recovery fixture'):
                with RunContext.resume(ref):
                    rows=fetch(tmp_path/'objects',[{'uri':url+'/file'}],durable=True)
                    assert rows[0]['result']['status']=='ok'
                    raise RuntimeError('repeat recovery fixture')
        assert calls['/file']==1
        with sqlite3.connect(tmp_path/'run/run.sqlite') as db:
            assert db.execute('SELECT disk,settled FROM runtime_raw_scopes').fetchall()==[(len(BODY),1)]
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='object_bytes'").fetchone()[0]==len(BODY)
            assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='raw_disk_bytes'").fetchone()[0]==len(BODY)
        assert not list((tmp_path/'objects').rglob('.pending/*'))
