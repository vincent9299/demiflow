"""Program identity records actual current definitions, without historical projection."""
from copy import deepcopy
from hashlib import sha256
import inspect
from pathlib import Path

from demiflow import data
from demiflow.execution.contracts.recovery_contract import code_record, identity
from demiflow.execution.seal.program_identity import describe

def _source(fn):
    return Path(inspect.getsourcefile(fn)).read_bytes()


def test_live_sdk_method_change_preserves_the_changed_function_and_class_pins(monkeypatch):
    fn = data.Dataset.join
    source_sha = sha256(_source(fn)).hexdigest()
    old_class_pins = describe(data.Dataset)['methods']
    altered = fn.__code__.replace(co_consts=(*fn.__code__.co_consts, 'modified'))
    monkeypatch.setattr(fn, '__code__', altered)
    actual_code = identity(code_record(altered))
    assert describe(fn)['code'] == actual_code
    changed_methods = describe(data.Dataset)['methods']
    assert {key for key in changed_methods if changed_methods[key] != old_class_pins[key]} == {'join'}
    assert describe(fn)['source_hash'] == source_sha
    assert describe(data.Dataset)['methods']['union'] == describe(data.Dataset.union)['code']


def test_class_pin_ignores_unrelated_file_content(monkeypatch, tmp_path):
    import demiflow.data.dataset as dataset_module
    before = describe(data.Dataset)
    alternate = tmp_path / 'dataset.py'
    alternate.write_bytes(_source(data.Dataset.join) + b'\n# unrelated edit\n')
    monkeypatch.setattr(dataset_module, '__file__', str(alternate))
    assert describe(data.Dataset) == before


def test_changed_result_cache_policy_always_uses_actual_source_and_code():
    from demiflow.data.result_cache import _state_policy
    captured = describe(_state_policy)
    assert captured['source_hash'] == sha256(_source(_state_policy)).hexdigest()
    assert captured['code'] == identity(code_record(_state_policy.__code__))


def test_sdk_identity_uses_actual_module_source_and_code():
    from demiflow.data.sources import KeyedSource, PlanInput, UnionSource, frozen_options
    for fn in (data.Dataset.join, data.Dataset.exclude_keys, frozen_options):
        actual = describe(fn)
        assert actual['module'] == fn.__module__
        assert actual['name'] == fn.__qualname__
        assert actual['source_hash'] == sha256(_source(fn)).hexdigest()
        assert actual['code'] == identity(code_record(fn.__code__))
    for kind in (data.Dataset, KeyedSource, PlanInput, UnionSource):
        assert describe(kind)['module'] == kind.__module__
        assert describe(kind)['methods'] == {
            name: identity(code_record(method.__code__))
            for name, method in vars(kind).items() if inspect.isfunction(method)}


def test_program_match_never_substitutes_an_earlier_source_or_code_digest():
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.seal.plan import _code_identity_from_value, _matches_original_program
    from demiflow.planning import dataflow_graph_adapter as routing
    for fn in (routing._identity, routing._tag, routing._unwrap):
        spec = CallableSpec.create(fn)
        current = _code_identity_from_value('callable', spec)
        assert _matches_original_program('callable', spec, current)
        for field in ('source_hash', 'code', 'module', 'name'):
            changed = deepcopy(current)
            target = changed['operations'][0]['fields']['callable']['fields']['target']
            target[field] = 'different-from-the-current-definition'
            assert not _matches_original_program('callable', spec, changed)


def test_current_sdk_nested_closure_roundtrips_with_its_real_identity(tmp_path):
    import cloudpickle
    def predicate(output):
        return data.from_items([{'id': 1, 'k': 'z'}]).deduplicate(
            'k', path=tmp_path / 'seen', key='id', output=output)._plan.operations[-1].callable.target
    with data.local_execution():
        original = predicate('dedup')
        different = predicate('second')
    assert describe(original)['source_hash'] == sha256(_source(original)).hexdigest()
    assert describe(cloudpickle.loads(cloudpickle.dumps(original))) == describe(original)
    assert describe(different) != describe(original)
