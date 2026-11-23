"""Content-bound SDK identity compatibility for runs sealed before D1/D2."""
from hashlib import sha256
import inspect
from pathlib import Path
from types import FunctionType

from demiflow import data
from demiflow.data.sources import frozen_options
from demiflow.execution.contracts.recovery_sdk_v3 import code_record
from demiflow.execution.recovery_contract import identity
from demiflow.execution.recovery_identity import describe
from demiflow.execution.recovery_sdk_v3 import function_pin


def _source(fn):
    return Path(inspect.getsourcefile(fn)).read_bytes()


def test_relational_sdk_v3_encodes_the_original_import_operand():
    dataset_pins = describe(data.Dataset)['methods']
    for name in ('join', 'reduce_by_key', 'group_batches', 'exclude_keys', 'union'):
        method = getattr(data.Dataset, name)
        names = method.__code__.co_names
        assert names.count('data.sources') == 1
        old_names = tuple('execution.local_kernel' if n == 'data.sources' else n for n in names)
        wire_code = identity(code_record(method.__code__.replace(co_names=old_names)))
        captured = describe(method)
        assert captured['source_hash'] == '1ba0e80f1ed0853948a97792dc65125fdaec17504bc3396b5f53ffb593da7e88'
        assert captured['code'] == dataset_pins[name] == wire_code


def test_unchanged_helpers_retain_their_v3_file_identities():
    frozen = describe(frozen_options)
    assert frozen['source_hash'] == '3033f1f14141c9759d29679ee5275492bfbc83bcd41b51c7bf9962afd115387f'
    assert frozen['code'] == identity(code_record(frozen_options.__code__))


def test_local_execution_keeps_post_retirement_source_and_code():
    # Retiring the old execution selectors changed this public context's
    # signature and body. It must not impersonate the pre-retirement helper.
    original = data.local_execution.__wrapped__
    binding = describe(data.local_execution)['closure'][0]
    # ④ changed import coordinates around the unchanged post-③ definition.
    assert binding['source_hash'] == 'cdb771da09e31f50a03e32459fc643b60b333e97cf1c0fbbf09d2e4ff6e70792'
    assert binding['source_hash'] != 'e67e3703f03ea14f8bcefb803bafc3251072e78bc17a5c1f2df60fad2ca2f83f'
    assert binding['code'] == identity(code_record(original.__code__))


def test_unrecognised_source_or_function_keeps_actual_evidence():
    source = _source(frozen_options)
    actual = sha256(source).hexdigest()
    other = sha256(source + b'\n').hexdigest()
    code = identity(code_record(frozen_options.__code__))
    assert function_pin(frozen_options, source + b'\n', other, code) == (other, code)
    clone = FunctionType(frozen_options.__code__, frozen_options.__globals__, frozen_options.__name__)
    clone.__qualname__ = frozen_options.__qualname__
    assert describe(clone)['source_hash'] == actual


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


def test_dataset_class_v3_qualified_import_in_optimized_python():
    import json
    import subprocess
    import sys

    probe = '''
import json
from demiflow import data
from demiflow.execution.contracts.recovery_sdk_v3 import code_record
from demiflow.execution.recovery_contract import identity
from demiflow.execution.recovery_identity import describe
fn = data.Dataset.join
old = tuple("execution.local_kernel" if name == "data.sources" else name for name in fn.__code__.co_names)
print(json.dumps([describe(data.Dataset)["methods"]["join"], identity(code_record(fn.__code__.replace(co_names=old)))]))
'''
    out = subprocess.check_output([sys.executable, '-OO', '-B', '-c', probe], text=True)
    actual, expected = json.loads(out)
    assert actual == expected


def test_sdk_created_nested_function_is_checked_before_and_after_pickle(tmp_path):
    import cloudpickle

    def predicate(output):
        rows = data.from_items([{'id': 1, 'k': 'z'}])
        return rows.deduplicate('k', path=tmp_path / 'seen', key='id',
                                output=output)._plan.operations[-1].callable.target

    with data.local_execution():
        live = predicate('dedup')
        different_closure = predicate('second')
    historical_file = '1ba0e80f1ed0853948a97792dc65125fdaec17504bc3396b5f53ffb593da7e88'
    assert describe(live)['source_hash'] == historical_file
    decoded = cloudpickle.loads(cloudpickle.dumps(live))
    assert decoded.__globals__ is not live.__globals__
    assert describe(decoded) == describe(live)
    assert describe(different_closure) != describe(live)
    same_file = tmp_path / 'same-module.py'
    same_file.symlink_to(decoded.__globals__['__file__'])
    decoded.__globals__['__file__'] = str(same_file)
    assert describe(decoded) == describe(live)
    separate_copy = tmp_path / 'different-module.py'
    separate_copy.write_bytes(same_file.read_bytes())
    decoded.__globals__['__file__'] = str(separate_copy)
    assert describe(decoded)['source_hash'] == sha256(_source(decoded)).hexdigest()
    # A genuine second installation has both coordinates pointing to its own
    # definition. The reader checks those bytes against its canonical source.
    saved = decoded.__code__
    decoded.__code__ = saved.replace(co_filename=str(separate_copy))
    assert describe(decoded) == describe(live)
    separate_copy.write_bytes(separate_copy.read_bytes() + b"\n# changed\n")
    assert describe(decoded)['source_hash'] == sha256(_source(decoded)).hexdigest()
    decoded.__code__ = saved
    decoded.__globals__['__file__'] = str(same_file)
    decoded.__globals__['unused'] = 123
    assert describe(decoded) == describe(live)
    decoded.__globals__['__package__'] = 'unrelated'
    assert describe(decoded)['source_hash'] == sha256(_source(decoded)).hexdigest()


def test_only_audited_original_routing_functions_accept_the_earlier_sealed_digest():
    from copy import deepcopy
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.seal.plan import _code_identity_from_value, _matches_original_program
    from demiflow.planning import dataflow_graph_adapter as routing

    recent = '0ab9e3db6f4dffe74c2f614ac887ce0a69749910b083db63eaa95c819652cbcf'
    earlier = 'cee316a5adf1fb7542a57efd9471ae4a1c836ae6d107a570ac5fc81b179fce16'
    for fn in (routing._identity, routing._tag, routing._unwrap):
        spec = CallableSpec.create(fn)
        current = _code_identity_from_value('callable', spec)
        old = deepcopy(current)
        target = old['operations'][0]['fields']['callable']['fields']['target']
        assert target['source_hash'] == recent
        target['source_hash'] = earlier
        assert _matches_original_program('callable', spec, old)
        assert not _matches_original_program('callable', CallableSpec.create(fn, fn_kwargs={'reader': 'changed'}), old)
        clone = FunctionType(fn.__code__, fn.__globals__, fn.__name__)
        assert not _matches_original_program('callable', CallableSpec.create(clone), old)
        other_scope = FunctionType(fn.__code__, dict(fn.__globals__), fn.__name__)
        assert not _matches_original_program('callable', CallableSpec.create(other_scope), old)
        source = _source(fn) + b'\n'
        changed_sha = sha256(source).hexdigest()
        code = identity(code_record(fn.__code__))
        assert function_pin(fn, source, changed_sha, code) == (changed_sha, code)
        target['code'] = 'f' * 64
        assert not _matches_original_program('callable', spec, old)
    assert describe(routing.graph_credit)['source_hash'] == sha256(_source(routing.graph_credit)).hexdigest()
    class_pin = describe(routing._ClassTarget)
    assert class_pin['methods']['__call__'] == identity(code_record(routing._ClassTarget.__call__.__code__))
