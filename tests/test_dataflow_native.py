"""execution.dataflow_native: the identity checks the compiler used to make itself.

The plan compiler no longer names AcquisitionCall, ProducerCall, NativeSearchCall,
StreamLanceFactory, PromptFactory, RoutedPromptFactory, CacheFactory,
QueueDatasource, QueueHistoryDatasource, LanceKeySource or QueueResultSink. It
asks this module. These tests pin that each answer is the same identity check
as before: a subclass or a look-alike with the same class path is not the class.
"""
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.data.plan import CallableSpec
from demiflow.execution.compiler_support import dataflow_native as native
from demiflow.execution.compiler_support.dataflow_native import NativeVariant

ROOT = Path(__file__).resolve().parents[1]


def _lookalike(cls):
    """A different class object with the same module path and qualified name."""
    twin = type(cls.__name__, (), {'__call__': lambda self, *args, **kwargs: None})
    twin.__module__, twin.__qualname__ = cls.__module__, cls.__qualname__
    return twin


def _subclass(cls):
    return type('Sub' + cls.__name__, (cls,), {})


def _declare(target, *args):
    # The compiler reads only .target and .constructor_args of a validated declaration.
    return SimpleNamespace(target=target, constructor_args=tuple(args))


def test_model_factories_are_recognised_by_identity_only():
    from demiflow.execution.operator_bindings.dataflow_model_factory import CacheFactory, EmbeddingFactory, PromptFactory, RoutedPromptFactory
    assert native.model_variant(None) is None
    assert native.model_variant(_declare(RoutedPromptFactory)) == 'routed_prompt'
    assert native.model_variant(_declare(PromptFactory)) == 'prompt'
    assert native.model_variant(_declare(CacheFactory)) == 'cache'
    assert native.model_variant(_declare(EmbeddingFactory)) is None      # lowered by its registered model compiler
    for cls in (RoutedPromptFactory, PromptFactory, CacheFactory):
        assert native.model_variant(_declare(_subclass(cls))) is None
        assert native.model_variant(_declare(_lookalike(cls))) is None


def test_native_prompt_admission_uses_only_the_actual_actor_class():
    from demiflow.operator_llm.native_actor import is_native_prompt_actor
    from demiflow.operator_llm.runtime import PromptActor

    actor = object.__new__(PromptActor)
    assert is_native_prompt_actor(actor)
    assert not is_native_prompt_actor(object.__new__(_subclass(PromptActor)))
    assert not is_native_prompt_actor(_lookalike(PromptActor)())
    assert not is_native_prompt_actor(actor.__call__)
    assert not is_native_prompt_actor(lambda row: row)


def test_service_declarations_and_the_configuration_they_add():
    from demiflow.execution.native_queue.dataflow_acquisition_call import AcquisitionCall
    from demiflow.execution.native_queue.dataflow_acquisition_call import ProducerCall
    from demiflow.execution.operator_bindings.dataflow_search import NativeSearchCall
    from demiflow.execution.native_queue.dataflow_stream_lance import StreamLanceFactory
    assert native.native_variant(None, None) is None
    assert native.native_variant(None, _declare(AcquisitionCall, 'finalize', True)) == NativeVariant(
        'acquisition', {'native_operation': 'finalize'})
    assert native.native_variant(None, _declare(ProducerCall)) == NativeVariant(
        'producer', {'native_operation': 'producer'})
    found = native.native_variant(None, _declare(NativeSearchCall, 'declared-key'))
    assert found == NativeVariant('search', {'native_operation': 'search', 'native_key': 'declared-key'})
    assert list(found.configuration) == ['native_operation', 'native_key']   # insertion order is kept
    assert native.native_variant(None, _declare(StreamLanceFactory, {}, None)).name == 'stream_write'
    assert native.native_variant(None, _declare(StreamLanceFactory, {}, len)).name == 'stream_write_predicate'
    assert native.native_variant(None, _declare(len)) is None
    for cls in (AcquisitionCall, ProducerCall, NativeSearchCall, StreamLanceFactory):
        assert native.native_variant(None, _declare(_subclass(cls), 'finalize', None)) is None
        assert native.native_variant(None, _declare(_lookalike(cls), 'finalize', None)) is None


def test_acquire_takes_its_key_from_the_declared_actor(tmp_path):
    from demiflow import data
    from demiflow.execution.native_queue.dataflow_fetch import fetch_declaration_key
    from demiflow.execution.operator_bindings.dataflow_model_factory import native_model_declaration
    from demiflow.objects import LocalObjectStore
    operation = data.from_items([{'request': 'r'}]).fetch(store=LocalObjectStore(str(tmp_path / 'objs')))._plan.operations[-1]
    variant = native.native_variant(operation, native_model_declaration(operation))
    actor = operation.callable.target.__self__
    assert variant == NativeVariant('acquisition', {'native_operation': 'acquire', 'native_key': fetch_declaration_key(actor)})


def test_trait_replacement_is_keyed_by_the_reported_names():
    from demiflow.planning.dataflow_compile import _VARIANT_TRAITS
    names = {native.ACQUISITION, native.PRODUCER, native.SEARCH, native.STREAM_WRITE, native.STREAM_WRITE_PREDICATE}
    assert set(_VARIANT_TRAITS) == {native.PRODUCER, native.SEARCH, native.STREAM_WRITE} < names
    assert set(_VARIANT_TRAITS.values()) == {('adapter', 'journaled', 'io', False)}
    # A user predicate has unproven state; acquisition keeps the traits of the operation that carries it.
    assert native.STREAM_WRITE_PREDICATE not in _VARIANT_TRAITS and native.ACQUISITION not in _VARIANT_TRAITS


def test_datasource_kind_is_exact_type_not_subclass():
    from demiflow.execution.sources.queue_history import QueueHistoryDatasource
    from demiflow.execution.native_queue.queue_source import QueueDatasource
    def instance(cls):
        return object.__new__(cls)       # the check reads the type only; no queue is opened
    assert native.datasource_kind(instance(QueueDatasource)) == 'queue'
    assert native.datasource_kind(instance(QueueHistoryDatasource)) == 'queue_history'
    assert native.datasource_kind(instance(_subclass(QueueDatasource))) == 'datasource'
    assert native.datasource_kind(instance(_subclass(QueueHistoryDatasource))) == 'datasource'
    assert native.datasource_kind(instance(_lookalike(QueueDatasource))) == 'datasource'
    assert native.datasource_kind(object()) == 'datasource'


def test_key_only_and_native_publication_accept_subclasses_as_before():
    from demiflow.execution.sources.lance_key_source import LanceKeySource
    from demiflow.execution.native_queue.queue_sinks import QueueResultSink
    def instance(cls):
        return object.__new__(cls)
    assert native.is_key_only(instance(LanceKeySource)) and native.is_key_only(instance(_subclass(LanceKeySource)))
    assert not native.is_key_only(instance(_lookalike(LanceKeySource))) and not native.is_key_only(object())
    assert not native.publishes_natively(instance(QueueResultSink))
    assert not native.publishes_natively(instance(_subclass(QueueResultSink)))
    assert native.publishes_natively(instance(_lookalike(QueueResultSink))) and native.publishes_natively(object())
    assert native.publishes_natively(None)


def test_narrowing_builds_the_key_only_source(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution.sources.lance_key_source import LanceKeySource
    uri = str(tmp_path / 't.lance')
    lance.write_dataset(pa.table({'k': ['a'], 'x': [1]}), uri)
    source = data.read_lance(uri)._source
    narrowed = native.narrow_to_keys(source, ['k'])
    assert type(narrowed) is LanceKeySource and narrowed.query.columns == ('k',)
    assert narrowed.query.uri == source.query.uri and source.query.columns != ('k',)


def test_cache_state_is_asked_of_the_owner_of_the_bound_callable():
    class Owner:
        def __init__(self, answer):
            self.answer, self.asked = answer, 0

        def validate_runtime_state(self):
            self.asked += 1
            return self.answer

        def __call__(self, row):
            return row
    for answer in (None, 'aliased state'):
        owner = Owner(answer)
        assert native.cache_runtime_state(CallableSpec.create(owner.__call__)) is answer and owner.asked == 1


def test_importing_the_module_initialises_no_native_service():
    script = (
        "import sys\n"
        "import demiflow.execution.compiler_support.dataflow_native as native\n"
        "native.model_variant(None); native.native_variant(None, None)\n"
        "from demiflow.operator_llm.native_actor import is_native_prompt_actor\n"
        "assert not is_native_prompt_actor(lambda row: row)\n"
        "heavy = [name for name in ('demiflow.execution.native_queue.dataflow_acquisition_call', 'demiflow.execution.native_queue.dataflow_queue',\n"
        "    'demiflow.execution.operator_bindings.dataflow_search', 'demiflow.execution.native_queue.dataflow_stream_lance', 'demiflow.execution.native_queue.dataflow_fetch',\n"
        "    'demiflow.execution.native_queue.queue_source', 'demiflow.execution.sources.queue_history', 'demiflow.execution.native_queue.queue_sinks',\n"
        "    'demiflow.execution.sources.lance_key_source', 'demiflow.operator_llm.runtime', 'demiflow.lance.search', 'lance', 'pyarrow')\n"
        "    if name in sys.modules]\n"
        "assert not heavy, heavy\n")
    done = subprocess.run([sys.executable, '-P', '-B', '-c', script], cwd=ROOT, capture_output=True, text=True, timeout=60,
                          env={'PYTHONPATH': str(ROOT), 'PATH': '/usr/bin:/bin'})
    assert done.returncode == 0, done.stdout + done.stderr


@pytest.mark.parametrize('name', [
    'AcquisitionCall', 'ProducerCall', 'NativeSearchCall', 'StreamLanceFactory', 'PromptFactory', 'RoutedPromptFactory',
    'CacheFactory', 'QueueDatasource', 'QueueHistoryDatasource', 'LanceKeySource', 'QueueResultSink'])
def test_the_compiler_no_longer_names_the_class(name):
    source = (ROOT / 'demiflow' / 'planning' / 'dataflow_compile.py').read_text(encoding='utf-8')
    assert name not in source


def test_every_registered_lowering_symbol_resolves_to_a_function_with_the_agreed_signature():
    """A symbol is a string an import scan cannot see; each one is resolved here for real."""
    import inspect
    from demiflow.execution.contracts.dataflow_kinds import kind_records
    from demiflow.planning import dataflow_compile
    from demiflow.planning.dataflow_compile import _GENERIC_LOWERING, _lowering, compile_kernel_spec
    compiler_symbols = {record.operator[0] for record in kind_records().values() if record.operator is not None}
    assert compiler_symbols == {_GENERIC_LOWERING, 'demiflow.planning.dataflow_models:build_model_factory'}
    assert _lowering(_GENERIC_LOWERING) is compile_kernel_spec is dataflow_compile.compile_kernel_spec
    for symbol in compiler_symbols:                      # lower(operation, *, program_ref, bounds)
        parameters = inspect.signature(_lowering(symbol)).parameters
        assert list(parameters) == ['operation', 'program_ref', 'bounds'], symbol
        assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in ('program_ref', 'bounds'))
    assert set(native.DECLARATION_LOWERINGS) == {native.ROUTED_PROMPT, native.PROMPT}
    assert native.declaration_lowering(native.CACHE) is None and native.declaration_lowering(None) is None
    for variant, symbol in native.DECLARATION_LOWERINGS.items():   # lower(operation, declaration, *, program_ref, bounds)
        assert native.declaration_lowering(variant) == symbol
        parameters = inspect.signature(_lowering(symbol)).parameters
        assert list(parameters) == ['operation', 'declaration', 'program_ref', 'bounds'], symbol
        assert all(parameters[name].kind is inspect.Parameter.KEYWORD_ONLY for name in ('program_ref', 'bounds'))


def test_only_engine_symbols_are_resolved():
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.planning.dataflow_compile import _lowering
    for symbol in ('os:system', 'json.decoder:JSONDecoder', 'demiflow.execution.operator_bindings.dataflow_models', 'build_model_factory',
                   'demiflow.execution.operator_bindings.dataflow_models:not_a_function', 'demiflow.execution.operator_bindings.dataflow_models:__doc__'):
        with pytest.raises(DataflowError) as refused:
            _lowering(symbol)
        assert refused.value.code == 'INVALID_CONTRACT', symbol


def test_kernel_symbols_name_real_functions_or_are_listed_as_not_resolvable():
    """Kernel symbols are registered but nothing resolves them today. This records which ones exist."""
    import importlib
    from demiflow.execution.contracts.dataflow_kinds import kind_records
    symbols = sorted({record.operator[1] for record in kind_records().values() if record.operator is not None})
    missing = []
    for symbol in symbols:
        module_name, _, name = symbol.partition(':')
        try:
            found = callable(getattr(importlib.import_module(module_name), name, None))
        except ImportError:
            found = False
        if not found:
            missing.append(symbol)
    assert len(symbols) == 5
    assert missing == KERNEL_SYMBOLS_WITHOUT_A_FUNCTION


KERNEL_SYMBOLS_WITHOUT_A_FUNCTION = []


@pytest.mark.parametrize('name', ['dataflow_models', 'dataflow_routed_prompt', 'dataflow_prompt_service',
                                  'build_model_factory', 'compile_routed_prompt', 'service_kernel', 'AgentMapOp'])
def test_the_generic_lowering_names_no_model_module(name):
    source = (ROOT / 'demiflow' / 'planning' / 'dataflow_compile.py').read_text(encoding='utf-8')
    assert name not in source
