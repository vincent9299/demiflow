"""Native collect declarations retain their fixed memory through v3 fitting."""
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.compiler_support.dataflow_native import collect_fixed_residency
from demiflow.planning.dataflow_budget import codec_window, default_profile
from demiflow.planning.dataflow_compile import compile_kernel_spec
from demiflow.planning.dataflow_policy import resolve_runtime_policy

MIB = 1024**2


def image_operation(concurrency=1):
    return data.from_items([{'raw': None}]).encode_image(column='raw', output='image',
        profile='oriented_white_bicubic_v1', memory_bytes=256*MIB,
        max_bytes=65536, concurrency=concurrency)._plan.operations[-1]


def bounds(memory=1024*MIB):
    return resolve_runtime_policy({}, {'memory_bytes': memory}, provider='local')[1]


@pytest.mark.parametrize('declared,actual', [(1, 1), (1, 2), (2, 1)])
def test_actual_concurrency_counts_fixed_workspace_once(declared, actual):
    operation = replace(image_operation(declared), concurrency=actual)
    fixed = actual * (256*MIB + 2*65536 + 32*MIB)
    assert collect_fixed_residency(operation, concurrency=actual) == fixed
    limits = bounds()
    profile = default_profile(operation, bounds=limits)
    plain = default_profile(None, bounds=limits, concurrency=actual)
    assert profile.requests['memory_bytes'] - plain.requests['memory_bytes'] == fixed
    kernel = compile_kernel_spec(operation, program_ref=None, bounds=limits)
    window = codec_window(kernel, operation, limits)
    minimum = default_profile(operation, bounds=limits, state=kernel.state,
                              codec_workspace=False).requests['memory_bytes']
    assert minimum >= fixed + actual*(limits.max_batch_bytes+limits.max_row_bytes)
    assert window is None or window[0] == minimum


def test_concurrent_images_cannot_fit_by_shrinking_payload_defaults():
    from demiflow.execution.recovery_contract import RecoveryContractError
    with pytest.raises(RecoveryContractError, match='Stream fixed residency exceeds'):
        default_profile(image_operation(2), bounds=bounds(512*MIB))


def test_explicit_worker_grant_cannot_omit_native_fixed_workspace():
    async_op = image_operation()
    operation = MapOp(async_op.callable, native_options=NativeOptions(
        backend='local', worker_resources={'memory_bytes': 128*MIB}))
    with pytest.raises(DataflowError, match='below the admitted working set'):
        default_profile(operation, bounds=bounds())


def test_unknown_actor_does_not_supply_compiler_resource_facts():
    class Unknown:
        @property
        def arguments(self):
            pytest.fail('unknown resource declaration was consulted')
        def __call__(self, row):
            pytest.fail('compiler executed a callback')
    operation = MapOp(CallableSpec.create(Unknown().__call__))
    assert collect_fixed_residency(operation, concurrency=3) == 0
    assert collect_fixed_residency(MapOp(CallableSpec.create(lambda x: x)), concurrency=3) == 0
