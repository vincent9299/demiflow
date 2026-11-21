"""Content-bound SDK identity compatibility for runs sealed before D1/D2."""
from hashlib import sha256
import inspect
from pathlib import Path
from types import FunctionType

from demiflow import data
from demiflow.data.sources import frozen_options
from demiflow.execution.artifacts import code_record
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
    binding = describe(data.local_execution)['closure'][0]
    assert binding['source_hash'] == 'e67e3703f03ea14f8bcefb803bafc3251072e78bc17a5c1f2df60fad2ca2f83f'


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
    altered = fn.__code__.replace(co_consts=(*fn.__code__.co_consts, 'modified'))
    monkeypatch.setattr(fn, '__code__', altered)
    actual_code = identity(code_record(altered))
    assert describe(fn)['code'] == describe(data.Dataset)['methods']['join'] == actual_code
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
