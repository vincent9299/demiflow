"""Which native declaration is this? Answered next to the classes that define it.

The plan compiler needs to know a few facts about native declarations: which
factory a validated declaration names, whether a datasource is a queue, whether
a Lance source reads keys only, whether a sink publishes natively. Each answer
is an identity check against one concrete class. Those checks live here, on the
execution side, so the compiler asks a question and never names the class.

Rules for this module:

* Every check is the same check the compiler made before: ``is`` stays ``is``,
  ``type(x) is`` stays ``type(x) is``, ``isinstance`` stays ``isinstance``. A
  class path string is not class identity and is never used here.
* Every class import is local to the function that needs it and happens only on
  the path that needed it before, so a plain CPU plan still initialises no
  native service module.
* Functions only. Nothing is added to the factory classes: their identity
  covers the code of each method, so a new method would change the identity of
  every plan that uses them.
* ``native_model_declaration`` remains the authority that validates a native
  declaration. This module only reads its result.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping

# The model factories the compiler lowers through a model path.
ROUTED_PROMPT = 'routed_prompt'
PROMPT = 'prompt'
CACHE = 'cache'
# Service declarations that add configuration to an ordinary kernel.
ACQUISITION = 'acquisition'
PRODUCER = 'producer'
SEARCH = 'search'
STREAM_WRITE = 'stream_write'                     # passive RPC factory, no user predicate
STREAM_WRITE_PREDICATE = 'stream_write_predicate'  # the same factory carrying a user predicate


# The lowering function of a recognised model declaration, as a symbol the plan
# compiler resolves on use:
#     lower(operation, declaration, *, program_ref, bounds) -> KernelSpec
# A cache declaration has none: its operation is lowered generically and only its
# state is tightened.
DECLARATION_LOWERINGS: Mapping[str, str] = MappingProxyType({
    ROUTED_PROMPT: 'demiflow.planning.dataflow_models:compile_routed_prompt',
    PROMPT: 'demiflow.planning.dataflow_models:lower_prompt_declaration',
})


@dataclass(frozen=True)
class NativeVariant:
    """A recognised service declaration and the configuration keys it adds, in order."""
    name: str
    configuration: Mapping[str, Any]


def model_variant(declaration) -> str | None:
    """Which model factory a validated native declaration names, if any."""
    if declaration is None:
        return None
    from demiflow.execution.dataflow_model_factory import CacheFactory, PromptFactory, RoutedPromptFactory
    if declaration.target is RoutedPromptFactory:
        return ROUTED_PROMPT
    if declaration.target is PromptFactory:
        return PROMPT
    if declaration.target is CacheFactory:
        return CACHE
    return None


def declaration_lowering(variant) -> str | None:
    """The registered lowering symbol of a model variant, or ``None`` for generic lowering."""
    return DECLARATION_LOWERINGS.get(variant)


def cache_runtime_state(callable_spec):
    """What the cache wrapper says about nested callback state; ``None`` means recoverable."""
    owner = getattr(callable_spec.target, '__self__', callable_spec.target)
    return owner.validate_runtime_state()


def native_variant(operation, declaration) -> NativeVariant | None:
    """Recognise a service declaration and compute the configuration it adds.

    The imports and the checks run in the order the compiler ran them, and every
    module is imported whether or not an earlier check matched, as before.
    """
    if declaration is None:
        return None
    name, configuration = None, {}
    from demiflow.execution.native_queue.dataflow_acquisition_call import AcquisitionCall
    from demiflow.execution.dataflow_producer import ProducerCall
    if declaration.target is AcquisitionCall:
        name = ACQUISITION
        configuration['native_operation'] = declaration.constructor_args[0]
        if configuration['native_operation'] == 'acquire':
            actor = getattr(operation.callable.target, '__self__', operation.callable.target)
            from demiflow.execution.dataflow_fetch import fetch_declaration_key
            configuration['native_key'] = fetch_declaration_key(actor)
    if declaration.target is ProducerCall:
        name = PRODUCER
        configuration['native_operation'] = 'producer'
    from demiflow.execution.dataflow_search import NativeSearchCall
    if declaration.target is NativeSearchCall:
        name = SEARCH
        configuration['native_operation'] = 'search'
        configuration['native_key'] = declaration.constructor_args[0]
    from demiflow.execution.dataflow_stream_lance import StreamLanceFactory
    if declaration.target is StreamLanceFactory:
        # With no predicate this is the verified passive RPC factory, not the
        # original driver writer object. A user predicate has unproven state.
        name = STREAM_WRITE if declaration.constructor_args[1] is None else STREAM_WRITE_PREDICATE
    return NativeVariant(name, configuration) if name is not None else None


def datasource_kind(datasource) -> str:
    """The source kind of a datasource: a queue, a queue history, or an ordinary datasource."""
    from demiflow.execution.queue_source import QueueDatasource
    from demiflow.execution.queue_history import QueueHistoryDatasource
    if type(datasource) is QueueDatasource:
        return 'queue'
    if type(datasource) is QueueHistoryDatasource:
        return 'queue_history'
    return 'datasource'


def is_key_only(source) -> bool:
    """Whether a Lance source was narrowed to its key columns."""
    from demiflow.execution.lance_key_source import LanceKeySource
    return isinstance(source, LanceKeySource)


def narrow_to_keys(source, columns):
    """The key-only reading of a Lance source, for the excluded side of exclude_keys."""
    from demiflow.execution.lance_key_source import LanceKeySource
    return LanceKeySource(replace(source.query, columns=tuple(columns)))


def publishes_natively(sink) -> bool:
    """Whether a datasink terminal publishes through the native path.

    Queue tasks were settled by their original authority already; that terminal
    only publishes the result manifest, without Arrow.
    """
    from demiflow.execution.queue_sinks import QueueResultSink
    return not isinstance(sink, QueueResultSink)


def collect_fixed_residency(operation, *, concurrency):
    """Fixed native collect workspace, counted once per actual concurrent call.

    Only exact platform actors supply facts. Never probe attributes of an
    unknown callable or execute its constructor while compiling a plan.
    """
    import types
    from demiflow.data.plan import CallableSpec
    declaration = getattr(operation, 'callable', None)
    if not isinstance(declaration, CallableSpec):
        return 0
    target = declaration.target
    actor = target.__self__ if type(target) is types.MethodType else target
    if actor is None or type(actor) in (types.FunctionType, types.BuiltinFunctionType):
        return 0
    from demiflow.objects import ReadLocalObject
    from demiflow.collect.documents import DecodeDocument
    from demiflow.collect.image_resize import EncodeImage
    from demiflow.collect.wiki_syntax import ParseWikitext
    if type(actor) not in (ReadLocalObject, DecodeDocument, EncodeImage, ParseWikitext):
        return 0
    from demiflow.collect.flow_contract import collect_facts
    facts = collect_facts(actor)
    # The original declaration already includes its concurrency. A public map
    # may override that value: use the actual scheduled concurrency exactly once.
    return facts['reserved_bytes'] // actor.arguments['concurrency'] * concurrency
