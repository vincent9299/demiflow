"""Finite local fixtures shared by the three source wiring examples."""
import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
from urllib.parse import urlsplit

from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.execution import RunContext
from demiflow.objects import LocalObjectStore,ObjectRef
from demiflow.queue import QueueChannel,QueueConfig,QueueLimits,QueueResultSink,HistoryReadLimits,ProducerSourceSpec,task_message

MAX_BYTES=65536
RUN_LIMITS={'memory_bytes':192*1024**2,'max_row_bytes':65536,'max_batch_bytes':65536}


@contextmanager
def fixture():
    """Two artifacts, a metadata document and one actual HTTP search endpoint."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workdir',type=Path)
    parser.add_argument('--explicit',action='store_true',help='settle through graph-tail ack_queue')
    args=parser.parse_args()
    root=args.workdir.absolute() if args.workdir else Path(tempfile.mkdtemp(prefix='demiflow-source-example-'))
    if args.workdir:root.mkdir(parents=True,exist_ok=False)
    calls=Counter();base={}
    objects={'/a':b'artifact-A','/b':b'artifact-B'}
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*args):pass
        def do_GET(self):
            path=urlsplit(self.path).path;calls[path]+=1
            if path=='/metadata':
                body=json.dumps({'items':[{'id':k[1:],'uri':base['url']+k} for k in objects]}).encode()
            elif path=='/search':
                body=json.dumps({'results':[{'url':base['url']+k,'title':k[1:],'content':'local fixture'} for k in objects]}).encode()
            else:body=objects.get(path,b'')
            self.send_response(200 if path in {*objects,'/metadata','/search'} else 404)
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    base['url']='http://127.0.0.1:'+str(server.server_port)
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.02));thread.start()
    config=QueueConfig(backend='sqlite',queue_id='source-example',path=str(root/'queue.sqlite'),
        limits=QueueLimits(max_tasks=8,max_workers=2,max_active_tasks=2,max_object_bytes=MAX_BYTES))
    try:yield root,base['url'],calls,QueueChannel(config),args.explicit
    finally:
        server.shutdown();server.server_close();thread.join(3)
        if thread.is_alive():raise RuntimeError('Example HTTP fixture did not stop')


def fixed_producer(channel,root,rows,*,kind,source_id):
    path=root/'source.jsonl'
    with path.open('x') as out:
        for row in rows:out.write(json.dumps(row,sort_keys=True)+'\n')
    if path.stat().st_size>8192:raise ValueError('Example source exceeds its finite byte budget')
    producer=channel.open('objects',producer_id='source',source=ProducerSourceSpec(kind=kind,
        source_id=source_id,uri=path.as_uri(),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),max_rows=8,max_bytes=8192))
    return path,producer


def request_messages(row):
    return {**row,'messages':[task_message(row['id'],{'id':row['id'],'request':{'uri':row['uri']},
        **({'metadata_raw_ref':row['metadata_raw_ref']} if 'metadata_raw_ref' in row else {})},pool='objects')]}


class Quality:
    """An ordinary stateless actor; raw receipt is preserved verbatim."""
    def __call__(self,row):
        result=row['result']
        raw=ObjectRef(**result['raw_ref']).read(max_bytes=MAX_BYTES) if result['status']=='ok' else b''
        return {**row,'acceptance':{'accepted':raw.startswith(b'artifact-'),'reason':None if raw else 'no_raw'}}


def summary(row):
    return {'id':row['id'],'status':row['result']['status'],'accepted':row['acceptance']['accepted']}


def accepted(row):return row['accepted']


def finish(channel,producer,root,calls,*,kind,explicit=False):
    channel.seal(producer)
    with RunContext.start(root/'consumer',limits=RUN_LIMITS):
        stream=(data.read_queue(channel.config,pool='objects',max_tasks=8,max_inflight=2,idle_timeout_s=1,
                               settlement='explicit' if explicit else 'sink')
            .fetch(store=LocalObjectStore(root/'raw'),concurrency=2,
                policy=FetchPolicy(max_object_bytes=MAX_BYTES,max_attempts=1,http=HttpSource(allow_private=True)))
            .map(Quality).select_columns(['id','result','acceptance','_queue_delivery']))
        if explicit:stream.ack_queue(channel.config,keep_row=False).run_stream()
        else:stream.write_datasink(QueueResultSink(channel.config,pool='objects'))
    snapshot=channel.snapshot('objects',kind='results')
    with RunContext.start(root/'postprocessing',limits=RUN_LIMITS):
        rows=(data.read_queue_records(channel.config,pool='objects',through=snapshot,results=True,
                    limits=HistoryReadLimits(max_rows=8,max_record_bytes=65536,max_total_record_bytes=512*1024,max_hash_bytes=8*MAX_BYTES))
              .map(summary).filter(accepted).take_all())
    assert len(rows)==2 and channel.state('objects')['queue_drained']
    assert calls['/a']==calls['/b']==1
    print(json.dumps(dict(source=kind,root=str(root),http_gets=dict(calls),results=rows,explicit=explicit),indent=2))
