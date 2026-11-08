"""Real HTTPS streaming with an independent COS signature verifier; no cloud I/O."""
from contextlib import contextmanager
import datetime
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import ipaddress
from pathlib import Path
import ssl
import threading
from urllib.parse import parse_qs,urlsplit,quote

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource,CosSource
from demiflow.objects import LocalObjectStore,ObjectRef
from demiflow.secrets import Secret


@contextmanager
def cos_origin(tmp_path,*,wrong_version=False,expired=False,hostname=False):
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'cos-fixture')])
    now=datetime.datetime.now(datetime.timezone.utc)
    certificate=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=2))
        .not_valid_after(now-datetime.timedelta(days=1) if expired else now+datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost')] if hostname else [x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
        .sign(key,hashes.SHA256()))
    cert=tmp_path/'ca.pem';private=tmp_path/'key.pem'
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    body=b'cos-fixture-raw-object';seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            parsed=urlsplit(self.path);params={k:v[0] for k,v in parse_qs(parsed.query).items()}
            auth=dict(p.split('=',1) for p in self.headers.get('authorization','').split('&'))
            canonical='&'.join(k.lower()+'='+quote(str(v),safe='') for k,v in sorted(params.items()))
            http='get\n'+parsed.path+'\n'+canonical+'\nhost='+self.headers['Host']+'\n'
            sign_key=hmac.new(b'synthetic-cos-key',auth['q-key-time'].encode(),hashlib.sha1).hexdigest()
            string='sha1\n'+auth['q-sign-time']+'\n'+hashlib.sha1(http.encode()).hexdigest()+'\n'
            signature=hmac.new(sign_key.encode(),string.encode(),hashlib.sha1).hexdigest()
            valid=(auth['q-signature']==signature and auth['q-ak']=='synthetic-cos-id' and
                auth['q-url-param-list']==';'.join(k.lower() for k in sorted(params)) and
                self.headers.get('x-cos-security-token')=='synthetic-session-token')
            seen.append({'path':parsed.path,'params':params,'valid':valid,'if_match':self.headers.get('if-match')})
            code=403 if not valid or parsed.path=='/denied' else 404 if parsed.path=='/missing' else 200
            self.send_response(code);self.send_header('Content-Length',str(len(body) if code==200 else 0))
            self.send_header('x-cos-version-id','wrong' if wrong_version else params.get('versionId','current'))
            self.send_header('ETag','"version-etag"');self.end_headers()
            if code==200:self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,private)
    server.socket=tls.wrap_socket(server.socket,server_side=True)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield f'https://127.0.0.1:{server.server_port}',cert,body,seen
    finally:server.shutdown();server.server_close();thread.join()


def policy(endpoint,cert,monkeypatch):
    for name,value in [('ID','synthetic-cos-id'),('KEY','synthetic-cos-key'),('TOKEN','synthetic-session-token')]:
        monkeypatch.setenv('DEMIFLOW_COS_FIXTURE_'+name,value)
    return FetchPolicy(max_object_bytes=1024,http=HttpSource(allow_private=True,ca_file=str(cert)),
        cos=(CosSource(bucket='fixture-bucket',region='fixture-region',endpoint=endpoint,
            secret_id=Secret('DEMIFLOW_COS_FIXTURE_ID'),secret_key=Secret('DEMIFLOW_COS_FIXTURE_KEY'),
            session_token=Secret('DEMIFLOW_COS_FIXTURE_TOKEN')),))


def test_cos_version_path_signing_if_match_and_known_failures(tmp_path,monkeypatch):
    with cos_origin(tmp_path) as (endpoint,cert,body,seen):
        p=policy(endpoint,cert,monkeypatch)
        requests=[{'uri':'cos://fixture-bucket/中文%20file','source_version':{'kind':'cos_version_id','value':'v1+&two'}},
            {'uri':'cos://fixture-bucket/etag','source_version':{'kind':'http_etag','value':'"version-etag"'}},
            {'uri':'cos://fixture-bucket/missing'},{'uri':'cos://fixture-bucket/denied'}]
        rows=data.from_items([{'id':i,'request':r} for i,r in enumerate(requests)]).fetch(store=LocalObjectStore(tmp_path/'objects'),policy=p).take_all()
        results={r['id']:r['result'] for r in rows}
        assert results[0]['status']=='ok' and ObjectRef(**results[0]['raw_ref']).read(max_bytes=1024)==body
        assert results[0]['source']['cos_version_id']=='v1+&two'
        assert results[1]['status']=='ok' and results[2]['reason']=='not_found' and results[3]['reason']=='access_denied'
        assert len(seen)==4 and all(r['valid'] for r in seen)
        assert any(r['if_match']=='"version-etag"' for r in seen)
        import json
        encoded=json.dumps(rows)
        for secret in ('synthetic-cos-id','synthetic-cos-key','synthetic-session-token'):assert secret not in encoded


@pytest.mark.parametrize('expired,hostname',[(True,False),(False,True)])
def test_remote_tls_rejection_is_a_terminal_receipt(tmp_path,monkeypatch,expired,hostname):
    with cos_origin(tmp_path,expired=expired,hostname=hostname) as (endpoint,cert,body,seen):
        rows=data.from_items([{'request':{'uri':'cos://fixture-bucket/item'}}]).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=policy(endpoint,cert,monkeypatch)).take_all()
        assert rows[0]['result']['reason']=='tls_peer_rejected' and rows[0]['result']['terminal']
        assert not seen


def test_cos_version_mismatch_never_falls_back_to_current(tmp_path,monkeypatch):
    with cos_origin(tmp_path,wrong_version=True) as (endpoint,cert,body,seen):
        row=data.from_items([{'request':{'uri':'cos://fixture-bucket/item','source_version':{'kind':'cos_version_id','value':'fixed'}}}]).fetch(
            store=LocalObjectStore(tmp_path/'objects'),policy=policy(endpoint,cert,monkeypatch)).take(1)[0]
        assert row['result']['reason']=='source_version_mismatch' and len(seen)==1
