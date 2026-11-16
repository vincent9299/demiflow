"""Native execution boundaries independent of the authoring checker."""
from dataclasses import replace
from pathlib import Path
import pytest
import pyarrow as pa

from demiflow import data
from demiflow.data.plan import CallableSpec, BoundMapOp, BoundCallable, StandardCallable
from demiflow.data.runtime_contract import check_size, check_value, RuntimeContractError
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.local_kernel import local_execution
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from demiflow.lance.model import LanceWriteSpec
from demiflow.lance.write import write_lance


@pytest.mark.parametrize('change', ['version', 'limit', 'output', 'extra_action'])
def test_actual_native_plan_must_match_before_consuming_data(tmp_path, change):
    source, target = str(tmp_path / 'absent-input.lance'), str(tmp_path / 'output.lance')
    nodes = [
        dict(id='s', kind='read_table', parameters=dict(uri=source, version=1, columns=None, filter=None, limit=None),
             normalized_parameters=dict(uri=source, version=1, columns=[], projection={}, filter=None, limit=None,
                                        batch_size=None, batch_readahead=None, fragment_readahead=None)),
        dict(id='l', kind='limit', parameters={'limit': 1}, normalized_parameters={'limit': 1},
             capability_fact={'kind': 'limit', 'driver': 'pull', 'native_options': False}, analysis={}),
        dict(id='w', kind='write_table', parameters=dict(uri=target, mode='create')),
    ]
    events = []
    context = CheckedExecution(nodes=nodes, edges=[dict(to='l', **{'from': 's'}, port=0), dict(to='w', **{'from': 'l'}, port=0)],
        actions=[] if change == 'extra_action' else ['w'], operators=[], source_root=tmp_path,
        limits=DEFAULT_LIMITS, record=lambda *event: events.append(event), check_stop=lambda: None)
    with local_execution(workers=1, worker_mode='thread', partitions=1):
        with pytest.raises(RuntimeContractError, match='differs|Unexpected'):
            with context.bind_context():
                ds = data.read_lance(source, version=2 if change == 'version' else 1)
                ds.limit(2 if change == 'limit' else 1).write_lance(
                    target + '-other' if change == 'output' else target, mode='create', return_receipt=True)
    assert not events and not Path(target).exists()


def test_actual_fields_null_optional_keys_and_bool_are_distinct():
    optional = dict(kind='record', fields={'x': dict(required=False, type=dict(kind='str', nullable=False))})
    check_value({}, optional, 'row')
    with pytest.raises(RuntimeContractError, match='null'):
        check_value({'x': None}, optional, 'row')
    required_null = dict(kind='record', fields={'x': dict(required=True, type=dict(kind='str', nullable=True))})
    check_value({'x': None}, required_null, 'row')
    with pytest.raises(RuntimeContractError, match='missing'):
        check_value({}, required_null, 'row')
    with pytest.raises(RuntimeContractError, match='int'):
        check_value(True, {'kind': 'int'}, 'value')


def test_field_mapping_validates_actual_consumed_and_returned_values():
    def increment(x, *, step=1):
        return x + step
    contract = dict(label='increment', max_row_bytes=1024,
        parameters=[dict(name='x', type={'kind': 'int'}), dict(name='step', type={'kind': 'int'})], returns={'kind': 'int'})
    op = BoundMapOp(CallableSpec(increment, runtime_contract=contract), {'x': 'source'}, output='result')
    assert BoundCallable(op)({'source': 2, 'keep': 'k'}) == {'source': 2, 'keep': 'k', 'result': 3}
    with pytest.raises(RuntimeContractError):
        BoundCallable(op)({'source': True})
    with pytest.raises(KeyError):
        BoundCallable(op)({'other': 2})
    with pytest.raises(RuntimeContractError, match='expected str'):
        BoundCallable(replace(op, callable=replace(op.callable, runtime_contract={**contract, 'returns': {'kind': 'str'}})))({'source': 2})


def test_recursive_wide_and_single_object_value_limits():
    with pytest.raises(RuntimeContractError, match='byte budget'):
        check_size({'huge': 'x' * 10000}, 1024)
    recursive = []
    recursive.append(recursive)
    with pytest.raises(RuntimeContractError, match='nesting'):
        check_size(recursive)


def test_native_writer_rejects_late_overflow_without_committed_version(tmp_path):
    import lance
    target = tmp_path / 'late-overflow.lance'
    def batches():
        yield pa.record_batch({'id': [1]})
        yield pa.record_batch({'id': [2, 3]})
    receipt = write_lance(LanceWriteSpec(str(target), mode='create'), batches(),
                          limits={**DEFAULT_LIMITS, 'max_output_rows': 2})
    assert receipt.status == 'indeterminate'
    with pytest.raises(Exception):
        lance.dataset(str(target))


def test_native_writer_bounds_parent_arrow_buffers_before_consumption(tmp_path):
    parent = pa.record_batch({'payload': ['x' * 8192, 'small']})
    tiny = parent.slice(1, 1)
    assert tiny.nbytes < 1024 and tiny.get_total_buffer_size() > 8192
    from demiflow.execution import RecoveryContractError
    # Borrowed parent buffers use the memory ceiling, not logical batch bytes.
    with pytest.raises(RecoveryContractError, match='retained buffers.*memory budget'):
        write_lance(LanceWriteSpec(str(tmp_path / 'slice.lance'), mode='create'), [tiny],
                    limits={**DEFAULT_LIMITS, 'memory_bytes': 4096,
                            'max_row_bytes': 512, 'max_batch_bytes': 1024})
