"""Owned benchmark worker: production runner plus temporary timing probes."""
import contextlib
import ctypes
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit, parse_qs

from profile_metrics import Metrics

# Permit only this owned benchmark parent (and its sampler children) to inspect us.
libc=ctypes.CDLL(None, use_errno=True)
if libc.prctl(0x59616d61, os.getppid(), 0, 0, 0)!=0:
    raise OSError(ctypes.get_errno(), 'Cannot authorize owned sampler')
root=Path(os.environ['DEMIFLOW_PROFILE_REPO']).resolve()
sys.path.append(str(root/'demiflow/services'))
import model_worker
metrics=Metrics()
original_make=model_worker.make_server


def instrument(adapter, *args, **kwargs):
    # Model loading has finished. Only measure requests, not startup imports/JIT.
    for name in ['single','_execute','_submit','_native','_replay','_capture']:
        if hasattr(adapter,name):
            metrics.wrap(adapter,name,'adapter.'+name,profile_root=name=='_execute')
    if hasattr(adapter,'validate'): metrics.wrap(adapter,'validate','platform.validate_request')
    if hasattr(adapter,'protocol'): metrics.wrap(adapter.protocol,'_prepare','native.prepare')
    if hasattr(adapter,'backend'): metrics.wrap(adapter.backend,'prepare','native.prepare')
    if hasattr(adapter,'decider'): metrics.wrap(adapter.decider.encoder,'plan','native.encode_plan')
    if hasattr(adapter,'runtime'): metrics.wrap(adapter.runtime,'native','native.vllm',profile_root=True)
    for module_name in ['pplx_graphs','native_graphs','jev_graphs']:
        module=sys.modules.get(module_name)
        if module and hasattr(module,'check_encoding_budget'):
            metrics.wrap(module,'check_encoding_budget','platform.encoding_budget')
    for name in ['observe','get','admit','make_room','put']:
        metrics.wrap(adapter.cache,name,'cache.'+name)
    enter=adapter.gate.enter
    @contextlib.contextmanager
    def gate(exclusive=False):
        scope=enter(exclusive=exclusive);start=time.perf_counter();cpu=time.thread_time()
        scope.__enter__()
        metrics.add('gate.exclusive_wait' if exclusive else 'gate.shared_wait',time.perf_counter()-start,time.thread_time()-cpu)
        try: yield
        finally: scope.__exit__(None,None,None)
    adapter.gate.enter=gate
    server=original_make(adapter,*args,**kwargs)
    handler=server.RequestHandlerClass
    metrics.wrap(handler,'do_POST','http.request',profile_root=True)
    metrics.wrap(handler,'reply','http.reply')
    get=handler.do_GET
    def control(self):
        url=urlsplit(self.path)
        if not url.path.startswith('/__benchmark/'):
            return get(self)
        if self.headers.get('X-Benchmark-Token')!=os.environ['DEMIFLOW_PROFILE_TOKEN']:
            return get(self)
        args=parse_qs(url.query)
        deadline=time.monotonic()+2
        while metrics.active and time.monotonic()<deadline:time.sleep(.002)
        # The benchmark owns this process and only calls control between phases.
        if url.path=='/__benchmark/reset':
            metrics.reset(False)  # CPython 3.12 cProfile tool IDs are process-global; use py-spy.
            body={'pid':os.getpid()}
        elif url.path=='/__benchmark/export':
            phase=args['phase'][0]
            if not phase.replace('-','').replace('_','').isalnum() or len(phase)>64:raise ValueError('Invalid phase')
            result=metrics.export(Path(os.environ['DEMIFLOW_PROFILE_OUTPUT'])/phase)
            body={'pid':os.getpid(),'stages':result['stages'],'profile_total_cpu_s':result.get('profile_total_cpu_s')}
        else:return get(self)
        # Avoid including benchmark control responses in production measurements.
        raw=json.dumps(body).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    handler.do_GET=control
    return server

model_worker.make_server=instrument
if __name__=='__main__':model_worker.main()
