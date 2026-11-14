"""S011 · unified_operation_traits — C04 ownership without changing legacy facts."""
from demiflow.data.plan import (
    CallableSpec, MapOp, FlatMapOp, FilterOp, SortOp,
)
from demiflow.planning.traits import unified_operation_traits, operation_traits


class Hooked:
    def __call__(self, row):
        return row

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass


class PlainData:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        return row


class EmptyHookless:
    def __call__(self, row):
        return row


def _fn(row):
    return row


def mapop(target):
    return MapOp(callable=CallableSpec.create(target))


def test_function_keeps_none_ownership():
    assert unified_operation_traits(mapop(_fn)).state_ownership == "none"


def test_class_factory_derives_shared_colocated():
    assert unified_operation_traits(mapop(PlainData)).state_ownership == "shared_colocated"


def test_hooks_instance_derives_shared_colocated():
    assert unified_operation_traits(mapop(Hooked())).state_ownership == "shared_colocated"


def test_hookless_instance_derives_auto_json_ownership():
    # Plain-data state is captured by auto_json at registration; sealed ownership
    # is not silently upgraded for a hookless instance.
    assert unified_operation_traits(mapop(PlainData())).state_ownership == "shared_colocated"
    assert unified_operation_traits(mapop(EmptyHookless())).state_ownership == "shared_colocated"


def test_legacy_cardinality_and_ordering_unchanged():
    assert unified_operation_traits(FlatMapOp(callable=CallableSpec.create(_fn))).cardinality == "many"
    assert unified_operation_traits(FilterOp(callable=CallableSpec.create(_fn))).cardinality == "zero_or_one"
    assert unified_operation_traits(SortOp(keys=("x",))).ordering == "requires_global_order"
    assert unified_operation_traits(SortOp(keys=("x",))).input_mode == "finite_scope"


def test_legacy_base_traits_preserved_for_function_map():
    base = operation_traits(mapop(_fn))
    unified = unified_operation_traits(mapop(_fn))
    assert unified.category == base.category
    assert unified.worker_model == base.worker_model
    assert unified.lifecycle == base.lifecycle
    assert unified.policy_class == base.policy_class