"""Standalone bounded HTTP runner; launched by file path in a model environment.

This script deliberately does not import the demiflow package or Dataset stack.
The experimental batch route exists only in explicit validation mode.
"""
import sys
from pathlib import Path

# Direct execution in an isolated model environment must not let the sibling
# services/http.py shadow Python's stdlib http package.
if not __package__:
    directory=str(Path(__file__).resolve().parent)
    sys.path[:]=[entry for entry in sys.path if str(Path(entry or '.').resolve())!=directory]
    sys.path.append(directory)

import argparse
from contextlib import nullcontext
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import subprocess
import threading

if __package__:
    from .model_adapters import PplxAdapter, JEVAdapter, AmosAdapter, D3Adapter, error_result
    from .quality import read_evidence, covered_request
else:
    from model_adapters import PplxAdapter, JEVAdapter, AmosAdapter, D3Adapter, error_result
    from quality import read_evidence, covered_request


MAX_BODY=16*1024*1024


def make_server(adapter, name, port, max_inflight, validation_batch_size=1, *,
                identity=None, batch_evidence=None, batch_max_size=1, api_key=None,
                backend_single_sequence=False, backend_serialized=False, batching=None):
    identity=identity or {'hardware':{'kind':'test'}}
    jev=isinstance(adapter,JEVAdapter)
    if backend_single_sequence and (not jev
            or identity.get('model_contract',{}).get('options',{}).get('max_num_seqs')!=1
            or batch_evidence or batch_max_size!=1 or validation_batch_size!=1):
        raise ValueError('Single-sequence pipelining requires JEV max_num_seqs=1 without batching')
    if backend_serialized and (not jev or backend_single_sequence
            or identity.get('model_contract',{}).get('options',{}).get('request_pipelining') is not True
            or getattr(adapter,'backend_serialized',False) is not True
            or batch_evidence or batch_max_size!=1 or validation_batch_size!=1):
        raise ValueError('Preparation pipelining requires a serialized owned JEV backend without batching')
    pipelined=backend_single_sequence or backend_serialized
    evidence,coverage = (read_evidence(batch_evidence,identity,batch_max_size,parallel=max_inflight,details=True)
                         if batch_evidence else (None,None))
    batch_size = batch_max_size if evidence else validation_batch_size
    if batch_size>1 and not callable(getattr(adapter,'batch',None)):
        raise ValueError('This model adapter does not support cross-request batch')
    if batching is not None:
        if __package__: from .batching import batching_config, BatchingAdapter
        else: from batching import batching_config, BatchingAdapter
        batching = batching_config(batching)
        if batch_size < batching['max_size']:
            raise ValueError('Service batching requires verified or explicit validation batch capacity')
        adapter = BatchingAdapter(adapter, batching)
    # vLLM may merge concurrent *single* HTTP calls internally. The same quality
    # gate must cover that path, even when no explicit batch route is requested.
    serialized=jev and not evidence and validation_batch_size==1 and not pipelined
    execution=threading.Lock() if serialized else nullcontext()
    capabilities={'single':True,'parallel':1 if serialized or pipelined or isinstance(adapter,(PplxAdapter,AmosAdapter,D3Adapter)) else None,
        'continuous_batching':bool(batching) or jev and not serialized and not pipelined,
        'request_pipelining':pipelined,'max_inflight':max_inflight,
        'model_revision':identity.get('model_contract',{}).get('revision'),
        'execution_identity':identity,'batch':{'supported':bool(evidence),'quality_verified':bool(evidence),
            'max_size':batch_max_size if evidence else 1,'evidence_sha256':evidence},
        'batch_validation':'verified' if evidence else 'pending','experimental_batch_size':validation_batch_size}
    if callable(getattr(adapter,'execution_status',None)):
        capabilities['parallel']=adapter.parallel
    capacity=threading.BoundedSemaphore(max_inflight)
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            self.connection.settimeout(120)
        def reply(self,status,body):
            data=bytearray()
            for part in json.JSONEncoder(ensure_ascii=False,allow_nan=False).iterencode(body):
                raw=part.encode()
                if len(data)+len(raw)>MAX_BODY:
                    status,data=502,b'{"error":{"detail":"Response too large"}}'; break
                data.extend(raw)
            self.send_response(status); self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
        def do_GET(self):
            if self.path=='/health':
                failed=getattr(adapter,'failed',None)
                return self.reply(503 if failed else 200,{'status':'failed' if failed else 'ready','model':name})
            if not self.authorized(): return
            if self.path=='/v1/models':
                current=dict(capabilities)
                if callable(getattr(adapter,'execution_status',None)):
                    current['execution']=adapter.execution_status()
                    current['parallel']=adapter.parallel
                return self.reply(200,{'data':[{'id':name,'object':'model','capabilities':current}]})
            self.reply(404,{'error':{'detail':'Unknown route'}})
        def do_POST(self):
            if not self.authorized(): return
            if not capacity.acquire(blocking=False):
                self.close_connection=True
                return self.reply(429,{'error':{'detail':'Service capacity exhausted'}})
            try:
                if self.headers.get('Transfer-Encoding'): raise ValueError('Content-Length is required')
                length=int(self.headers.get('Content-Length','-1'))
                body_limit = min(MAX_BODY, getattr(adapter,'max_request_bytes',MAX_BODY)) if self.path=='/v1/systemone' else MAX_BODY
                if not 0<length<=body_limit:
                    self.close_connection=True
                    return self.reply(413,{'error':{'detail':'Request body exceeds limit'}})
                raw=self.rfile.read(length)
                if len(raw)!=length: raise ValueError('Incomplete request body')
                body=json.loads(raw)
                if not isinstance(body,dict) or body.get('model')!=name:
                    raise ValueError('Model differs from deployed model')
                if self.path=='/v1/systemone':
                    if (jev or batching) and evidence and not covered_request(body,coverage):
                        raise ValueError('Concurrent execution evidence does not cover this input')
                    with execution: result=adapter.single(body)
                    return self.reply(200,{'model':name,**result})
                if self.path=='/v1/systemone_batch' and batch_size>1:
                    requests=body.get('requests')
                    if (set(body)!={'model','requests'} or not isinstance(requests,list)
                            or not 1<=len(requests)<=batch_size):
                        raise ValueError('Invalid or oversized batch')
                    results=[None]*len(requests); valid=[]
                    for index,request in enumerate(requests):
                        try:
                            if not isinstance(request,dict) or request.get('model',name)!=name:
                                raise ValueError('Batch item model differs')
                            if coverage is not None and not covered_request(request,coverage):
                                raise ValueError('Batch evidence does not cover this input type or option count')
                            valid.append((index,request))
                        except (ValueError,KeyError,TypeError) as exc: results[index]=error_result(exc)
                    if valid:
                        values=adapter.batch([request for _,request in valid])
                        if len(values)!=len(valid): raise RuntimeError('Adapter batch response count differs')
                        for (index,_),value in zip(valid,values,strict=True): results[index]=value
                    return self.reply(200,{'model':name,'responses':results,
                                           'evidence_sha256':evidence})
                self.reply(404,{'error':{'detail':'Route unavailable; production batch requires quality validation'}})
            except (ValueError,TypeError,KeyError,RecursionError) as exc:
                self.close_connection=True
                self.reply(422,error_result(exc))
            except Exception as exc:
                self.close_connection=True
                self.reply(503,error_result(exc,503))
            finally: capacity.release()
        def authorized(self):
            if api_key is None or hmac.compare_digest(
                    self.headers.get('Authorization','').encode(),('Bearer '+api_key).encode()):
                return True
            self.close_connection=True
            self.reply(401,{'error':{'detail':'Bearer credentials required'}})
            return False
        def log_message(self,*args): pass
    class Server(ThreadingHTTPServer):
        daemon_threads=False
        block_on_close=True
        def __init__(self,*args):
            self.connections=threading.BoundedSemaphore(max_inflight+4)
            super().__init__(*args)
        def process_request(self,request,address):
            if not self.connections.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try: super().process_request(request,address)
            except BaseException:
                self.connections.release(); raise
        def process_request_thread(self,request,address):
            try: super().process_request_thread(request,address)
            finally: self.connections.release()
    try:
        server = Server(('127.0.0.1',port),Handler)
    except BaseException:
        if batching: adapter.close()
        raise
    server.adapter = adapter
    return server


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',required=True,choices=['pplx','autotrust-jev','amos','d3'])
    parser.add_argument('--model',required=True,type=Path); parser.add_argument('--name',required=True)
    parser.add_argument('--port',required=True,type=int); parser.add_argument('--backend')
    parser.add_argument('--max-inflight',type=int,default=8)
    parser.add_argument('--validation-batch-size',type=int,default=1)
    parser.add_argument('--cpu-threads',type=int,default=8)
    parser.add_argument('--max-length',type=int)
    parser.add_argument('--identity',required=True)
    parser.add_argument('--batch-evidence')
    parser.add_argument('--batch-max-size',type=int,default=1)
    parser.add_argument('--backend-single-sequence',action='store_true')
    parser.add_argument('--backend-serialized',action='store_true')
    parser.add_argument('--cuda-graphs')
    parser.add_argument('--pplx-fast')
    parser.add_argument('--batching')
    parser.add_argument('--native-reference',action='store_true',
                        help='Use the bounded backend with graphs disabled for independent validation')
    parser.add_argument('--jev-options')
    args=parser.parse_args()
    if not 1<=args.max_inflight<=512 or not 1<=args.validation_batch_size<=64:
        parser.error('Invalid service capacity')
    client=None
    graph_settings=None
    fast_settings=None
    if args.pplx_fast:
        if args.profile != 'pplx' or args.cuda_graphs or args.native_reference:
            parser.error('pplx-fast requires its own native pplx execution recipe')
        if __package__:
            from .pplx_fast import fast_config
            from .cuda_graphs import configure_allocator
        else:
            from pplx_fast import fast_config
            from cuda_graphs import configure_allocator
        fast_settings=fast_config(json.loads(args.pplx_fast))
        configure_allocator(fast_settings)
    if args.batching and fast_settings is None:
        parser.error('Service batching currently requires pplx-fast')
    if args.native_reference and not args.cuda_graphs:
        parser.error('Native reference requires the bounded CUDA backend')
    if args.cuda_graphs:
        if args.profile not in {'pplx','amos','d3','autotrust-jev'} or args.batch_evidence or args.validation_batch_size!=1:
            parser.error('CUDA graphs require native single-request execution')
        if __package__:
            from .cuda_graphs import graph_config, configure_allocator
        else:
            from cuda_graphs import graph_config, configure_allocator
        graph_settings=graph_config(json.loads(args.cuda_graphs))
        if args.native_reference and (graph_settings['streams']!=1 or args.max_inflight!=1):
            parser.error('Native reference requires one stream and one admitted request')
        if graph_settings['streams']>args.max_inflight: parser.error('CUDA streams exceed HTTP admission')
        configure_allocator(graph_settings)
    # JEV's wrapper does not acquire another GPU lock or load model weights;
    # inspect backend device identities only during this explicit GPU launch.
    if args.profile=='autotrust-jev' and graph_settings is None:
        os.environ['CUDA_VISIBLE_DEVICES']=os.environ['DEMIFLOW_BACKEND_GPUS']
    import torch
    if graph_settings is not None and graph_settings['streams'] > 1:
        if __package__: from .triton_dispatch import install
        else: from triton_dispatch import install
        install()
    if not 1 <= args.cpu_threads <= 64: parser.error('cpu-threads must be 1..64')
    torch.set_num_threads(args.cpu_threads)
    identity=json.loads(args.identity)
    identity['hardware']={'kind':'cuda','cuda':torch.version.cuda,
        'devices':[{'name':torch.cuda.get_device_properties(i).name,
                    'uuid':str(getattr(torch.cuda.get_device_properties(i),'uuid','unknown')),
                    'capability':list(torch.cuda.get_device_capability(i))}
                   for i in range(torch.cuda.device_count())]}
    # Selected GPUs only; the report is invalidated by a driver change too.
    driver=subprocess.run(['nvidia-smi','--query-gpu=uuid,driver_version',
        '--format=csv,noheader','--id='+os.environ['CUDA_VISIBLE_DEVICES']],
        capture_output=True,check=True,timeout=10).stdout
    if len(driver)>16384: raise ValueError('GPU metadata exceeds limit')
    identity['hardware']['driver']=driver.decode().strip().splitlines()
    if args.profile=='pplx':
        from pplx_decider import Decider
        kwargs = {'max_length':args.max_length} if args.max_length is not None else {}
        decider=Decider.from_pretrained(args.model,device='cuda',cpu_threads=args.cpu_threads,**kwargs)
        if fast_settings is not None:
            if __package__: from .pplx_fast import PplxFastAdapter
            else: from pplx_fast import PplxFastAdapter
            adapter=PplxFastAdapter(decider,fast_settings)
        elif graph_settings is not None:
            if __package__: from .pplx_graphs import PplxGraphAdapter
            else: from pplx_graphs import PplxGraphAdapter
            adapter=PplxGraphAdapter(decider,graph_settings)
        else: adapter=PplxAdapter(decider)
    elif args.profile=='amos':
        from amos.serve import load_engine
        engine=load_engine(str(args.model),'cuda',args.name,max_len=args.max_length,strict=True)
        adapter=AmosAdapter(engine)
        if graph_settings is not None:
            if __package__: from .native_graphs import NativeGraphAdapter, AmosBackend
            else: from native_graphs import NativeGraphAdapter, AmosBackend
            adapter=NativeGraphAdapter(AmosBackend(engine),graph_settings)
    elif args.profile=='d3':
        sys.path.insert(0,str(args.model))
        from d3_server import D3, Decider, Refusal
        model=D3.from_pretrained(str(args.model),device='cuda:0',verify='fast')
        model.model_name=args.name
        model.warmup()
        class NativeDecider(Decider):
            def answer(self,body):
                try: return super().answer(body)
                except Refusal as exc: raise ValueError(str(exc.detail)) from exc
        decider=NativeDecider(model)
        adapter=D3Adapter(decider)
        if graph_settings is not None:
            if __package__: from .native_graphs import NativeGraphAdapter, D3Backend
            else: from native_graphs import NativeGraphAdapter, D3Backend
            adapter=NativeGraphAdapter(D3Backend(decider),graph_settings)
    else:
        import importlib.util
        import httpx
        from transformers import AutoTokenizer
        spec=importlib.util.spec_from_file_location('jev_format',args.model/'jev_format.py')
        formatter=importlib.util.module_from_spec(spec); spec.loader.exec_module(formatter)
        readout=json.loads((args.model/'vllm_readout.json').read_text())
        tokenizer=AutoTokenizer.from_pretrained(args.model)
        if graph_settings is not None:
            if args.backend or args.backend_single_sequence or args.backend_serialized:
                parser.error('JEV graph execution owns its native engine')
            if __package__:
                from .jev_graphs import JEVGraphAdapter
                from .model_adapters import JEVProtocol
            else:
                from jev_graphs import JEVGraphAdapter
                from model_adapters import JEVProtocol
            options=json.loads(args.jev_options or '{}')
            protocol=JEVProtocol(formatter,tokenizer,readout,options.get('max_model_len',32768))
            adapter=JEVGraphAdapter(protocol,args.model,options,graph_settings)
        else:
            client=httpx.Client(timeout=600,trust_env=False,
                limits=httpx.Limits(max_connections=args.max_inflight,max_keepalive_connections=args.max_inflight))
            adapter=JEVAdapter(formatter,tokenizer,readout,args.backend,args.name,client,
                               max_model_len=args.max_length or 32768,serialize_backend=args.backend_serialized or args.backend_single_sequence)
    if args.native_reference:
        adapter.cache.disabled = True
        adapter.mode = args.profile + '_native_reference'
    server=make_server(adapter,args.name,args.port,args.max_inflight,args.validation_batch_size,
        identity=identity,batch_evidence=args.batch_evidence,batch_max_size=args.batch_max_size,
        api_key=os.environ.get('DECISION_API_KEY') or None if args.profile=='d3' else None,
        backend_single_sequence=args.backend_single_sequence,backend_serialized=args.backend_serialized,
        batching=json.loads(args.batching) if args.batching else None)
    try: server.serve_forever()
    finally:
        server.server_close()
        if callable(getattr(server.adapter,'close',None)): server.adapter.close()
        if client is not None: client.close()


if __name__=='__main__': main()
