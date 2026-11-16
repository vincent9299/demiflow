"""Actual owned HTTP processes; only the external model transport is a fixture."""
import json
import socket
import sys
import time

import psutil

from demiflow.embeddings import EmbeddingModel
from demiflow.services import ManagedHTTPService


_SERVER = '''import json, os, signal, threading, time, subprocess, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import psutil
port, ledger, mode = int(__import__('sys').argv[1]), Path(__import__('sys').argv[2]), __import__('sys').argv[3]
def record(event):
    entry = dict(event=event, pid=os.getpid(), birth=psutil.Process().create_time(), cuda=os.environ.get('CUDA_VISIBLE_DEVICES'), at_ns=time.time_ns())
    fd=os.open(ledger, os.O_WRONLY|os.O_CREAT|os.O_APPEND, 0o600)
    try: os.write(fd, (json.dumps(entry)+'\\n').encode()); os.fsync(fd)
    finally: os.close(fd)
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503 if mode=='unready' else 200); self.end_headers()
        self.wfile.write(b'{"data":[{"id":"fixture"}]}')
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        record('request')
        if mode=='slow': time.sleep(2)
        self.send_response(503 if mode=='failure' else 200); self.end_headers()
        try:
            self.wfile.write(json.dumps({'model':'fixture','data':[
                {'index':i,'embedding':[3,4,0]} for i in range(len(body['input']))]}).encode())
        except (BrokenPipeError, ConnectionResetError): pass
    def log_message(self,*args): pass
server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
if mode=='detached':
    # Default close_fds intentionally drops the unit FD. The living subreaper
    # must retain ownership and find this separate-session descendant itself.
    child_code="import json,os,psutil,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); fd=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600); os.write(fd,(json.dumps(dict(event='descendant',pid=os.getpid(),birth=psutil.Process().create_time(),cuda=os.environ.get('CUDA_VISIBLE_DEVICES'),at_ns=time.time_ns()))+'\\\\n').encode()); os.fsync(fd); os.close(fd); time.sleep(60)"
    subprocess.Popen([sys.executable,'-c',child_code,str(ledger)],start_new_session=True)
def stop(*args):
    record('terminate')
    threading.Thread(target=server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,stop)
record('start')
try: server.serve_forever(poll_interval=.02)
finally: server.server_close(); record('exit')
'''


def managed_fixture(root, monkeypatch, *, mode='success', port=None, request=None):
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('DEMIFLOW_RESOURCE_HOME', str(root / 'service-port-locks'))
    if port is None:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
    script, ledger = root / 'embedding_server.py', root / 'events.jsonl'
    script.write_text(_SERVER)
    endpoint = f'http://127.0.0.1:{port}/v1'
    service = ManagedHTTPService([sys.executable, str(script), str(port), str(ledger), mode],
        base_url=endpoint, expected_model='fixture', root=root,
        startup_timeout_s=5,
        shutdown_timeout_s=.5, readiness_timeout_s=.2, poll_interval_s=.01,
        resource_directory=root / 'port-locks')
    fixture = {'model': EmbeddingModel('fixture', 'fixed', 3, endpoint),
               'service': service, 'events': ledger, 'script': script}
    if request is not None:
        request.addfinalizer(lambda: cleanup_owned(fixture))
    return fixture


def events(fixture):
    path = fixture['events']
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def cleanup_owned(fixture):
    """Emergency teardown after assertions; never substitutes for exit evidence."""
    import os
    import signal
    for item in events(fixture):
        if item['event'] not in ('start', 'descendant'):
            continue
        try:
            process = psutil.Process(item['pid'])
            if process.create_time() != item['birth']:
                continue
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=2)
            except psutil.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass


def assert_reaped(fixture, *, starts=1, requests=1):
    rows = events(fixture)
    launched = [item for item in rows if item['event'] == 'start']
    assert len(launched) == starts, rows
    assert sum(item['event'] == 'request' for item in rows) == requests, rows
    for item in [row for row in rows if row['event'] in ('start', 'descendant')]:
        assert item['cuda'] == ''  # CPU service cannot claim any host GPU.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                process = psutil.Process(item['pid'])
                if process.create_time() != item['birth']:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(.02)
        else:
            raise AssertionError(f"Owned service process remains: {item}")
    return rows
