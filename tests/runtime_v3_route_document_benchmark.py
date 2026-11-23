"""Local CPU/wall microbenchmark: original route+publish vs joint encoding."""
from demiflow.execution.contracts import record_codec
import hashlib,json,statistics,subprocess,time
from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.exchange import dataflow_channels as channels
from demiflow.execution.exchange import dataflow_bridge as codec
from demiflow.execution.exchange.dataflow_state import RoutingCounters
from test_runtime_v3_c import bounds,make


def main():
    result=[]
    limits=bounds(max_row_bytes=2**20)
    for name,row,iterations in [('narrow',{'k':1,'s':'中文','x':-0.0},10000),
                                ('wide_8k',{'k':1,'left':'x'*8192,'right':'y'*8192},2000)]:
        payload=record_codec._encode_payload(row,max_bytes=limits.max_row_bytes)
        record=Record('a'*64,{'ordinal':7},None,row,len(payload),7)
        raw=record_codec._encode_record_document(record,max_bytes=limits.max_row_bytes)
        for routing,readers in [('forward',('r0',)),('broadcast',('r0','r1','r2'))]:
            spec=make('ExchangeSpec',channel_id='bench',edge_id='b'*64,producer_partitions=('p0',),
                      consumer_partitions=readers,mode='memory_incremental',routing=routing,
                      credit_bytes=limits.max_row_bytes)
            old_state=RoutingCounters(values={},route_epoch=0)
            new_state=RoutingCounters(values={},route_epoch=0)
            expected=channels.route_record(record,spec=spec,state=old_state)
            actual,encoded=channels._route_record_document(record,spec=spec,state=new_state,bounds=limits)
            assert actual==expected and encoded==raw
            def old():
                routes=channels.route_record(record,spec=spec,state=old_state)
                for route in routes:record_codec._encode_record_document(route.record,max_bytes=limits.max_row_bytes)
            def new():channels._route_record_document(record,spec=spec,state=new_state,bounds=limits)
            timings={label:[] for label in ('old','new')}
            for _ in range(50):old();new()
            for repeat in range(3):
                for label,fn in [('old',old),('new',new)][::1 if repeat%2==0 else -1]:
                    wall,cpu=time.perf_counter(),time.process_time()
                    for _ in range(iterations):fn()
                    timings[label].append({'wall':time.perf_counter()-wall,'cpu':time.process_time()-cpu})
            med={label:{clock:statistics.median(v[clock] for v in values) for clock in ('wall','cpu')}
                 for label,values in timings.items()}
            result.append({'case':name+'_'+routing,'iterations':iterations,'samples':timings,'median_seconds':med,
                           'cpu_speedup':med['old']['cpu']/med['new']['cpu'],'wall_speedup':med['old']['wall']/med['new']['wall'],
                           'wire_sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
    print(json.dumps({'base_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
          'comparison':'same current canonical encoder; original two-step route then per-reader publish versus joint one-encode path',
          'cache':'none; both paths receive same immutable input and fresh sequential routing ordinals',
          'results':result},indent=2))

if __name__=='__main__':main()
