"""Two rules carried over from the retired 11-10 graph worker tests (P8 batch 1).

The v3 code has both rules; no v3 test asserted them:

  - data that carries another route epoch is fenced at delivery
    (was tests/test_u3_credit.py::test_crd_old_route_epoch_data_fenced);
  - a control request that gets no reply before its deadline stays unknown and is sent once
    (was tests/test_upgrade_v2_unit_L035.py::test_timeout_keeps_cut_unknown_without_resend).
"""
from types import SimpleNamespace as NS

import pytest

from demiflow.execution import dataflow_hosts as hosts
from demiflow.execution.contracts.dataflow import ExchangeSpec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import identity
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from test_runtime_v3_h import BIG, mk_event


def _memory_consumer(route_epoch):
    spec = ExchangeSpec(channel_id='ch', edge_id=identity(['edge', 0]), producer_partitions=('p',),
                        consumer_partitions=('c',), mode='memory_incremental', routing='forward', keys=(),
                        scope_id=None, route_epoch=route_epoch, credit_rows=256, credit_bytes=262144,
                        storage_bytes=0, read_domain='local', reason='test')
    runner = object.__new__(WorkerGraphRunner)
    runner.partition = 'c'
    runner._exchange = {spec.edge_id: spec}
    runner._memory_frontiers = {}
    runner.e = NS(_graph=NS(edges=[NS(edge_id=spec.edge_id, consumer='op')]), _node_specs={'op': NS(kind='transform')})
    return runner, spec


@pytest.mark.parametrize('command_epoch', [0, 2, None])
def test_memory_delivery_from_another_route_epoch_is_fenced(command_epoch):
    runner, spec = _memory_consumer(route_epoch=1)
    command = {'edge_id': spec.edge_id, 'producer_partition': 'p', 'route_sequence': 0}
    if command_epoch is not None:
        command['route_epoch'] = command_epoch
    with pytest.raises(DataflowError, match='original routing frontier') as failure:
        runner._receive_data(command)
    assert failure.value.code == 'FENCED'
    assert runner._memory_frontiers == {}  # the fenced delivery did not move the frontier


def test_control_request_without_a_reply_stays_unknown_and_is_sent_once():
    mailbox = hosts.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    event = mk_event('contribution')
    with pytest.raises(DataflowError) as failure:
        mailbox.request(event, deadline_ms=50)
    assert failure.value.code == 'UNKNOWN_EFFECT'
    sent = mailbox.drain(max_events=8)
    assert [item.operation_id for item in sent] == [event.operation_id]
    assert len(mailbox.drain(max_events=8)) == 0  # the deadline did not queue a second copy
