"""Bounded local HTTP fixture, incremental producer and two native worker processes.

python -B examples/raw_fetch_queue.py
python -B examples/raw_fetch_queue.py --backend postgresql
The latter requires DEMIFLOW_EXAMPLE_QUEUE_DSN and demiflow[queue-postgresql].
This demonstrates one host; it does not certify a multi-host shared object mount.
"""
import argparse
from collections import Counter
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import uuid

from demiflow import data
from demiflow.collect import FetchPolicy, HttpSource, Secret
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore
from demiflow.queue import QueueChannel, QueueConfig, QueueLimits, QueueResultSink, ProducerSourceSpec, HistoryReadLimits, task_message


def configuration(root):
    path=root/'queue.json'
    if path.stat().st_size>65536:raise ValueError('Example configuration exceeds its byte budget')
    values=json.loads(path.read_text())
    if values['dsn'] is not None:values['dsn']=Secret(values['dsn']['secret_env'])
    return QueueConfig(**values)


def worker(root, index):
    config=configuration(root)
    with RunContext.start(root/('worker-'+str(index)),limits={
            'memory_bytes':96*1024**2,'max_row_bytes':65536,'max_batch_bytes':65536}):
        (data.read_queue(config,pool='objects',max_tasks=8,max_inflight=2,idle_timeout_s=30)
            .fetch(store=LocalObjectStore(root/('raw-'+str(index))),concurrency=2,
                policy=FetchPolicy(max_object_bytes=65536,max_attempts=1,http=HttpSource(allow_private=True)))
            .write_datasink(QueueResultSink(config,pool='objects')))


def demonstrate(root, backend):
    limits=QueueLimits(max_tasks=16,max_workers=4,max_active_tasks=4,max_object_bytes=65536)
    config=(QueueConfig(backend='sqlite',queue_id='example',path=str(root/'queue.sqlite'),limits=limits)
        if backend=='sqlite' else QueueConfig(backend='postgresql',queue_id='example',
            dsn=Secret('DEMIFLOW_EXAMPLE_QUEUE_DSN'),namespace='example_'+uuid.uuid4().hex,limits=limits))
    (root/'queue.json').write_text(json.dumps(config.declaration()))
    channel=QueueChannel(config)
    producer=channel.open('objects',producer_id='fixed-fixture',source=ProducerSourceSpec(
        kind='records',source_id='fixture-v1',uri='memory:local-http-fixture',version=1,max_rows=8,max_bytes=65536))
    body=bytes(range(256))*256
    digest=hashlib.sha256(body).hexdigest()
    calls=Counter()
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def do_GET(self):
            calls[self.path]+=1
            payload=b'' if self.path=='/missing' else body
            self.send_response(404 if self.path=='/missing' else 200)
            self.send_header('Content-Length',str(len(payload)));self.end_headers()
            self.wfile.write(payload)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    serving=threading.Thread(target=lambda:server.serve_forever(poll_interval=.02));serving.start()
    children=[]
    try:
        # Start independent workers while the producer is still open and empty.
        for index in range(2):
            log=(root/('worker-'+str(index)+'.log')).open('w')
            try:
                child=subprocess.Popen([sys.executable,'-B',str(Path(__file__).absolute()),
                    '--worker',str(index),'--workdir',str(root)],stdout=log,stderr=subprocess.STDOUT)
            finally:log.close()
            children.append(child)
        url='http://127.0.0.1:'+str(server.server_address[1])
        rows=[{'messages':[task_message('object-'+str(index),{'id':index,'request':{
            'uri':url+('/missing' if index==7 else '/file'),
            'expected_size':len(body),'expected_sha256':digest}},pool='objects')]} for index in range(8)]
        with RunContext.start(root/'producer'):
            data.from_items(rows).enqueue(config,producer=producer).run_stream()
        channel.seal(producer)
        for child in children:
            if child.wait(timeout=60)!=0:raise RuntimeError('Worker failed; inspect its bounded local log')
        state=channel.state('objects')
        assert state['queue_drained'] and state['counts']=={'done_ok':7,'done_failed':1}
        snapshot=channel.snapshot('objects',kind='results')
        history=data.read_queue_records(config,pool='objects',through=snapshot,results=True,
            limits=HistoryReadLimits(max_rows=8,max_total_record_bytes=1024**2,max_hash_bytes=8*65536)).take_all()
        assert len(history)==8 and sum(calls.values())==8
        print(json.dumps(dict(root=str(root),backend=backend,worker_processes=2,
            completed=state['counts'],http_gets=sum(calls.values()),history_rows=len(history)),indent=2))
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:child.kill();child.wait(timeout=5)
        server.shutdown();server.server_close();serving.join(3)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--backend',choices=['sqlite','postgresql'],default='sqlite')
    parser.add_argument('--workdir',type=Path)
    parser.add_argument('--worker',type=int,choices=[0,1])
    args=parser.parse_args()
    if args.worker is not None:
        if args.workdir is None:parser.error('--worker requires --workdir')
        worker(args.workdir.absolute(),args.worker)
    else:
        if args.workdir is None:root=Path(tempfile.mkdtemp(prefix='demiflow-fetch-example-'))
        else:root=args.workdir.absolute();root.mkdir(parents=True,exist_ok=False)
        demonstrate(root,args.backend)
