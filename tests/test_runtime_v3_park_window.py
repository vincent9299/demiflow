"""Parking fills original bounded windows; explicit pause still freezes finish."""
from collections import deque
from types import SimpleNamespace as NS
import threading

import pytest

from demiflow.execution.contracts.dataflow import Bounds, ExchangeSpec
from demiflow.execution.exchange.dataflow_state import ChannelSetState
from demiflow.execution.recovery_contract import identity
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner


def runner(*, fanout=1, rows=128):
    r=object.__new__(WorkerGraphRunner)
    r.bounds=Bounds(4096,16384,4096,64,67108864,32768,65536,16*1024**2,32*1024**2,524288,4096,256,200000,120000)
    r._condition=threading.Condition();r._wake_generation=0
    r._credits={};r._credit_payloads={};r._closed_outputs=set();r._credit_limit=64
    r._queue=deque();r._park_mode=True;r._paused=False;r._park_requested=False;r._park_finish_pending=False
    r._idle_sent=False;r._cut=None;r._source_rows_since_cut=0;r._consumed_deliveries=set()
    r.partition='p';r._finished=set();r._finished_ports=set();r._active_finishes={}
    specs=tuple(ExchangeSpec('ch'+str(i),identity(['edge',i]),('p',),('c'+str(i),),
        'durable_incremental','forward',(),None,0,256,262144,2097152,'local','test') for i in range(fanout))
    r._exchange={s.edge_id:s for s in specs}
    r._outgoing={'op':[NS(edge_id=s.edge_id,consumer='c'+str(i),input_port='main') for i,s in enumerate(specs)]}
    channels=ChannelSetState(specs,bounds=r.bounds)
    r.outputs=[]
    def finish(port,wait_for_calls=False):
        yield from range(rows)
    kernel=NS(pending_calls=False,cooperative_finish=True,finish_replayable=True,finish=finish)
    r.e=NS(_node_specs={'op':NS(kind='relational')},_channels=channels,
        _kernels={('op','p'):kernel},_cancel_event=threading.Event())
    incoming=NS(edge_id='input',producer='producer',consumer='op',input_port='main')
    r._incoming={'op':[incoming]};r._finished_inputs={('input','p')}
    def output(occ,record):
        r.outputs.append(record)
        for spec in specs:
            key=identity([spec.edge_id,len(r.outputs)])
            lease=channels.reserve(spec.channel_id,spec.consumer_partitions[0],rows=1,size_bytes=4096,operation_id=key)
            r._credits[key]=(lease,None);r._credit_payloads[key]={'record':record}
    r._output=output;r._finish_node=lambda occ:r._finished.add(occ)
    return r


def advance(r):
    return r._advance_input_completions(max_records=1,deadline_ms=50)


def test_park_drains_replayable_finish_to_original_byte_window_then_real_ack_reopens():
    r=runner();r.receive({'kind':'park'})
    for _ in range(57):
        assert advance(r)==1
    assert r.outputs==list(range(57))
    assert not r._park_finish_pending
    assert advance(r)==0
    credit=r.e._channels.reader_credit('ch0','c0')
    assert credit['bytes_left']==262144-57*4096
    assert len(r._credits)==57
    # Real receive ACK is the only operation that returns the old leases.
    for key in list(r._credits):r.receive({'kind':'ack','delivery_id':key})
    assert not r.e._channels.outstanding_leases()
    assert advance(r)==1 and r.outputs[-1]==57


def test_explicit_pause_does_not_drain_replayable_finish():
    r=runner();r.receive({'kind':'park'});assert advance(r)==1
    r.receive({'kind':'pause'})
    assert advance(r)==0 and r.outputs==[0]


def test_source_fanout_uses_real_64_slots_without_guessing_smaller_row_count():
    r=runner(fanout=2)
    for n in range(32):
        assert r._park_can_output('op')
        r._output('op',n)
    assert len(r._credits)==64
    assert not r._park_can_output('op')
    assert all(lease.size_bytes==4096 for lease in r.e._channels.outstanding_leases())


def test_finish_eof_reaches_completion_without_empty_output_spin():
    r=runner(rows=0);r.receive({'kind':'park'})
    assert advance(r)==2
    assert r._finished=={'op'} and not r._park_finish_pending
    assert not r._credits


def test_resume_before_delayed_park_still_stops_at_original_credit_window():
    r=runner();r.receive({'kind':'resume'})
    for _ in range(57):assert advance(r)==1
    assert advance(r)==0
    assert r._paused and r.outputs==list(range(57))
    assert len(r._credits)==57
