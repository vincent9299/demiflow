"""Compile corpus for the compile <-> runtime contract work.

Each case builds a plan through the real declaration path (the public Dataset
entries, with the same arguments the entry tests use) and lowers it with the
real compiler. Nothing is executed: no source is read, no actor is started.

Two families:

* ``CASES`` build an ``ActionRequest`` directly, with fixed bounds, a fixed
  action identity and, where a case says so, a stand-in executor (the three
  attributes the compiler reads for defaults) or a stand-in report context (the
  three mappings the compiler reads). They do not depend on the machine.
* ``CAPTURED`` run the real terminal method under ``capture_pipeline``, the
  production "compile without executing" entry. The request, the executor, the
  policy and the bounds are the ones a real run would use on this machine.

Two uses:

* ``dump(root)`` compiles every case and returns, per case, the SHA-256 of the
  fully encoded graph (every KernelSpec field, not only what ``dag_id`` covers),
  the program declarations, the report bindings and the observation labels in
  full. Running it once with the baseline engine tree and once with a changed
  tree, against the same ``root``, shows byte for byte whether a compiler change
  altered any output. Nothing is normalised.
* ``structure(result)`` keeps shapes only, for the committed baseline.

A case that the compiler refuses is recorded with its error; the refusal is part
of the contract and is compared like any other outcome.

The callables used by the cases live in ``compile_contract_functions.py``.
"""
import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

from compile_contract_functions import (
    Counter, FixedDatasource, FixedSink, Session, batch, batch_async, build_inputs, connection, double, fold,
    identity, identity_async, keep, many, route, rows_factory,
)

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: v1
    model:
      name: mock-model
      transport: openai_compatible
      base_url_env: TEST_PROMPT_URL
      api_key_env: TEST_PROMPT_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: |
      {{ payload | json }}
'''
ROWS = [{'x': 1, 'k': 'a', 'item': {'x': 1}, 'text': 'hi'}, {'x': 2, 'k': 'b', 'item': {'x': 2}, 'text': 'yo'}]


class Context:
    """Fixtures at fixed locations under ``root`` so two engine trees see the same inputs."""

    def __init__(self, root):
        import lance
        import pyarrow as pa
        from demiflow.embeddings import EmbeddingModel
        from demiflow.operator_llm.parser import parse_prompt_pack
        self.root = Path(root).resolve()   # Lance and object stores need absolute locations
        self.root.mkdir(parents=True, exist_ok=True)
        self.pack = parse_prompt_pack(PACK)
        self.model = EmbeddingModel('fixture', 'fixed-revision', 3, 'http://127.0.0.1:1/v1')
        self.lance_uri = str(self.root / 't.lance')
        if not Path(self.lance_uri).exists():
            lance.write_dataset(pa.table({'k': ['a', 'b'], 'vec': [[1.0, 2.0], [3.0, 4.0]], 'x': [1, 2]}),
                                self.lance_uri)
        for name, text in (('rows.jsonl', '{"x": 1, "k": "a"}\n{"x": 2, "k": "b"}\n'), ('rows.csv', 'x,k\n1,a\n2,b\n')):
            if not (self.root / name).exists():
                (self.root / name).write_text(text, encoding='utf-8')
        (self.root / 'objroot').mkdir(exist_ok=True)
        if not (self.root / 'prompt.yaml').exists():
            (self.root / 'prompt.yaml').write_text(PACK.replace('base_url_env: TEST_PROMPT_URL',
                                                               'base_url: http://127.0.0.1:1/v1'), encoding='utf-8')
        # The search adapter fixture is resolved by module name, as the search tests do.
        for tests in (Path(__file__).resolve().parent, Path(__import__('demiflow').__file__).resolve().parents[1] / 'tests'):
            if (tests / 'native_search_fixtures').is_dir() and str(tests / 'native_search_fixtures') not in sys.path:
                sys.path.append(str(tests / 'native_search_fixtures'))
                break

    def rows(self, rows=None):
        from demiflow import data
        return data.from_items(ROWS if rows is None else rows)

    def path(self, name):
        return str(self.root / name)

    def lance(self, **options):
        from demiflow import data
        return data.read_lance(self.lance_uri, **options)

    def queue(self):
        from demiflow.execution.queue_contract import QueueConfig
        return QueueConfig(backend='sqlite', queue_id='q', path=self.path('q.db'))

    def producer(self):
        from demiflow.execution.queue_contract import ProducerRef, ProducerSourceSpec
        source = ProducerSourceSpec(kind='records', source_id='src', uri='file:///corpus/x', version=1)
        return ProducerRef(queue_uuid='qu', pool='pool', generation='gen', producer_id='pid', source=source,
                           epoch=0, token='tok')

    def library(self):
        from demiflow.collect.document_library import DocumentLibrary
        return DocumentLibrary(index_path=self.path('idx'), object_directory=self.path('docobjs'))

    def reading(self):
        from demiflow.collect.reading import PromptContext
        from demiflow.operator_llm.tokens import CharacterBudget
        return PromptContext(prompt=self.pack.prompt_definitions['enrich'], budget=CharacterBudget(6000),
                             build_inputs=build_inputs)

    def agent(self):
        from demiflow.agent import AgentConfig
        from demiflow.environment import OperatorEnvironment
        return AgentConfig(self.pack, OperatorEnvironment(resources='resources'), {}, 1000)

    def schema(self):
        import pyarrow as pa
        return pa.schema([('k', pa.string()), ('x', pa.int64())])

    def store(self):
        from demiflow.objects import LocalObjectStore
        return LocalObjectStore(self.path('objs'))

    def web(self):
        from demiflow.collect import SearchConfig
        from demiflow.collect.session import WebSession
        engine = {'name': 'alpha', 'module': 'adapter', 'revision': 'fixture-v1', 'base_url': 'http://127.0.0.1:1/alpha',
                  'enable_http': True, 'weight': 1}
        return WebSession(cache_path=self.root / 'web-cache.sqlite', object_directory=self.root / 'web-objects',
                          search=SearchConfig(engines=(engine,), language='en', host_interval_s=0, workers=1,
                                              max_bytes=65536, max_results=8))

    def service(self, model='fixture'):
        from demiflow.services import ManagedHTTPService
        script = self.root / 'service.py'
        if not script.exists():
            script.write_text('raise SystemExit("the compile corpus never starts a service")\n', encoding='utf-8')
        return ManagedHTTPService([sys.executable, str(script)], base_url='http://127.0.0.1:59999/v1',
                                  expected_model=model, root=self.root, startup_timeout_s=5, shutdown_timeout_s=.5,
                                  readiness_timeout_s=.2, poll_interval_s=.01, resource_directory=self.root / 'port-locks')

    def service_pack(self):
        from demiflow.operator_llm.parser import parse_prompt_pack
        return parse_prompt_pack(PACK.replace('base_url_env: TEST_PROMPT_URL', 'base_url: http://127.0.0.1:59999/v1'))

    def snapshot(self):
        from demiflow.execution.queue_contract import QueueSnapshot
        return QueueSnapshot(queue_uuid='qu', pool='default', generation='gen', kind='tasks', cursor=1)


# -- stand-ins for the two ambient inputs of the compiler -----------------------
class _Kernel:
    def __init__(self, batch_rows, partitions):
        self.batch_rows, self.partitions = batch_rows, partitions


class _Executor:
    def __init__(self, local, block_size=None):
        self._local_kernel, self._block_size = local, block_size


EXECUTORS = {
    'local': lambda: _Executor(_Kernel(512, 3)),
    'local_without_rows': lambda: _Executor(_Kernel(None, None), 64),
    'block_only': lambda: _Executor(None, 128),
}


class _Report:
    """The three mappings ``_Capture`` reads from the checked-execution context."""

    def __init__(self):
        self.source_bindings, self.operation_bindings, self.observations = {}, {}, {}


def _walk(source, plan, seen):
    """Every (source, operation) object reachable from a request, in a stable order.

    A source reached a second time is not walked again, but the operations of the
    plan that reached it are: a report context binds every operation of every
    branch, including branches that share a source.
    """
    if id(source) not in seen:
        seen[id(source)] = source
        yield 'source', source
        for name in ('left', 'right'):
            child = getattr(source, name, None)
            if child is not None and hasattr(child, 'plan'):
                yield from _walk(child.source, child.plan, seen)
        for child in getattr(source, 'inputs', ()) or ():
            yield from _walk(child.source, child.plan, seen)
    for operation in plan.operations:
        if id(operation) in seen:
            continue
        seen[id(operation)] = operation
        yield 'operation', operation
        dataset = getattr(operation, 'source_dataset', None)
        if dataset is not None:
            yield from _walk(dataset._source, dataset._plan, seen)


def report_for(source, plan, action_id, *, terminal=True):
    report = _Report()
    for index, (role, value) in enumerate(_walk(source, plan, {})):
        key = (action_id, 'report-' + role + '-' + str(index), index % 2)
        (report.source_bindings if role == 'source' else report.operation_bindings)[id(value)] = key
        report.observations[key] = {'state': 'reused' if index % 3 == 1 else 'running'}
    if terminal:
        report.observations[(action_id, action_id, 0)] = {'state': 'running'}
    return report


# -- bounds -----------------------------------------------------------------------
def _policy(limits):
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    return resolve_runtime_policy({}, limits, provider='local')[1]


BOUNDS = {
    'default': lambda: _policy({}),
    # The default image declaration reserves 1632 MiB before payload/state.
    'image_4g': lambda: _policy({'memory_bytes': 4 * 1024**3}),
    'm192_r8_b32': lambda: _policy({'memory_bytes': 192 * 1024, 'max_row_bytes': 8 * 1024, 'max_batch_bytes': 32 * 1024}),
    'm256_r16_b32': lambda: _policy({'memory_bytes': 256 * 1024, 'max_row_bytes': 16 * 1024, 'max_batch_bytes': 32 * 1024}),
    'm64_r8_b20': lambda: _policy({'memory_bytes': 64 * 1024, 'max_row_bytes': 8 * 1024, 'max_batch_bytes': 20 * 1024}),
    'm80_r8_b20': lambda: _policy({'memory_bytes': 80 * 1024, 'max_row_bytes': 8 * 1024, 'max_batch_bytes': 20 * 1024}),
    'm80_r8_b16_s128': lambda: _policy({'memory_bytes': 80 * 1024, 'max_row_bytes': 8 * 1024, 'max_batch_bytes': 16 * 1024,
                                        'max_state_bytes': 128 * 1024}),
    'm192_state3071': lambda: replace(_policy({'memory_bytes': 192 * 1024, 'max_row_bytes': 8 * 1024,
                                              'max_batch_bytes': 32 * 1024}), max_state_bytes=3071),
    'row4k_batch8k_state16k': lambda: replace(_policy({}), max_row_bytes=4096, max_batch_bytes=8192, max_state_bytes=16384),
    'partitions2': lambda: replace(_policy({}), max_partitions=2),
    'nodes3': lambda: replace(_policy({}), max_nodes=3),
    'depth1': lambda: replace(_policy({}), max_depth=1),
}


def bounds():
    return BOUNDS['default']()


# -- cases --------------------------------------------------------------------------
def case(dataset, kind='take', arguments=None, **extra):
    """A direct request. ``extra``: sink, programs, bounds, executor, report, source, plan."""
    if arguments is None:
        arguments = {'limit': 2} if kind == 'take' else {}
    return dict(dataset=dataset, kind=kind, arguments=arguments, **extra)


def _data():
    from demiflow import data
    return data


def _resources(**resources):
    return {'ray': {'worker_resources': resources}}


def _aggregates():
    from demiflow.data.aggregate import Count, Sum
    from demiflow.execution.contracts.dataflow import ActionProgram
    values = (Count(), Sum('x'))
    return tuple(ActionProgram(name='aggregate-' + str(index), kind='aggregate', value=value)
                 for index, value in enumerate(values))


def _queue_sink(c):
    from demiflow.execution.queue_sinks import QueueResultSink
    return QueueResultSink(c.queue(), pool='default', result='result')


def _graph(c, routing, **options):
    """A routed graph exactly as execution.executors.graph.materialize_graph builds it."""
    from demiflow.data.plan import LogicalPlan
    from demiflow.planning.dataflow_graph_adapter import translate_graph
    base = options.pop('base', None) or c.rows()
    ancestor = base.map(identity)._plan if options.pop('ancestor', True) else LogicalPlan()
    targets = [{'reader_id': 'first', 'plan': LogicalPlan(base.map(identity)._plan.operations[-1:])},
               {'reader_id': 'second', 'plan': LogicalPlan()}]
    if options.pop('actor_target', False):
        targets[0] = {'reader_id': 'first', 'plan': LogicalPlan(base.map(Counter)._plan.operations[-1:])}
    spec = {'source': base._source, 'ancestor': {'plan': ancestor}, 'targets': targets, 'routing': routing, **options}
    source = translate_graph(spec)
    return dict(dataset=None, source=source, plan=LogicalPlan(), kind='materialize',
                arguments={'graph': source.declaration})


def _routed_prompt(c, **options):
    return c.rows([{'item': {'x': 1}, 'which': 'first'}]).map_prompt_async(
        'enrich', config=c.pack, inputs={'payload': 'item'}, output='answer',
        routes={'first': {'concurrency': 1}, 'second': {'concurrency': 1}}, route='which', **options)


def _fit_predict(c):
    from demiflow import data
    from training_action_programs import StepPredictor, managed_trainer

    predictor = data.PredictorSpec(factory=StepPredictor, inputs={'x': 'x'}, input_schema={'x': 'int64'},
                                   output_schema={'x': 'int64', 'step': 'int64'}, batch_format='pyarrow')
    recipe = data.TrainingRecipe(trainer=managed_trainer, predictor=predictor, train_schema={'x': 'int64'},
                                 base_model_digest='a' * 64, environment_digest='b' * 64, model_format='native.test',
                                 budget={'optimizer_updates': 2}, save_steps=(1, 2),
                                 topology={'nodes': 1, 'world_size': 1, 'gpus_per_node': 0})
    fitted = c.lance(version=1).fit(recipe=recipe)
    return case(fitted.checkpoints.select(steps=[1, 2]).predict(on=c.rows()))


CASES = {
    # -- typed operations ---------------------------------------------------
    'map': lambda c: case(c.rows().map(identity)),
    'map_bound': lambda c: case(c.rows().map(double, inputs=['x'], output='y')),
    'map_bound_outputs': lambda c: case(c.rows().map(double, inputs={'x': 'x'}, outputs={'y': 'y'})),
    'map_fn_args': lambda c: case(c.rows().map(identity, fn_kwargs={})),
    'flat_map': lambda c: case(c.rows().flat_map(many)),
    'filter': lambda c: case(c.rows().filter(keep)),
    'fit_predict': _fit_predict,
    'map_batches': lambda c: case(c.rows().map_batches(batch)),
    'map_batches_sized': lambda c: case(c.rows().map_batches(batch, batch_size=4)),
    'map_stateful_instance': lambda c: case(c.rows().map(Counter())),
    'map_class_factory': lambda c: case(c.rows().map(Counter)),
    'map_streaming': lambda c: case(c.rows().map(identity, concurrency=2)),
    'map_streaming_labelled': lambda c: case(c.rows().map(identity, concurrency=2, queue_depth=4, label='named')),
    'map_streaming_worker_scope': lambda c: case(c.rows().map(Counter, concurrency=2, callable_scope='worker')),
    'map_streaming_catch': lambda c: case(c.rows().map(identity, concurrency=1, catch=(ValueError,))),
    'map_streaming_thread': lambda c: case(c.rows().map(identity, concurrency=2, execution='thread')),
    'map_async': lambda c: case(c.rows().map_async(identity_async, concurrency=2)),
    'map_async_labelled': lambda c: case(c.rows().map_async(identity_async, concurrency=2, label='named', catch=(ValueError,))),
    'batch_map': lambda c: case(c.rows().batch_map(batch_async, max_batch=4)),
    'batch_map_flush': lambda c: case(c.rows().batch_map(batch_async, max_batch=4, flush_interval=1.0, concurrency=2)),
    'select_drop_rename': lambda c: case(c.rows().select_columns(['x', 'k']).rename_columns({'x': 'y'})),
    'add_column': lambda c: case(c.rows().add_column('z', identity)),
    'drop_columns': lambda c: case(c.rows().drop_columns(['text'])),
    'limit': lambda c: case(c.rows().limit(1)),
    'random_sample': lambda c: case(c.rows().random_sample(0.5, seed=1)),
    'sort': lambda c: case(c.rows().sort('x')),
    'sort_descending': lambda c: case(c.rows().sort('x', descending=True)),
    'repartition': lambda c: case(c.rows().repartition(num_blocks=2)),
    'repartition_shuffle_keys': lambda c: case(c.rows().repartition(num_blocks=2, shuffle=True, keys=['k'])),
    'repartition_keys_without_shuffle': lambda c: case(c.rows().repartition(num_blocks=2, keys=['k'])),
    'repartition_over_partitions': lambda c: case(c.rows().repartition(num_blocks=4), bounds='partitions2'),
    'random_shuffle': lambda c: case(c.rows().random_shuffle(seed=1)),
    'random_shuffle_blocks': lambda c: case(c.rows().random_shuffle(seed=1, num_blocks=2)),
    'randomize_block_order': lambda c: case(c.rows().randomize_block_order(seed=1)),
    'chain_map_filter_map': lambda c: case(c.rows().map(identity).filter(keep).map(identity)),
    'chain_task_group': lambda c: case(c.rows().map(identity).select_columns(['x']).filter(keep).random_sample(0.5, seed=1)
                                       .map(Counter).drop_columns(['k'])),
    'chain_labels_repeat': lambda c: case(c.rows().map(identity).map(identity).map(identity)),
    'shared_prefix_union': lambda c: (lambda base: case(base.map(identity).union(base.filter(keep))))(c.rows().map(identity)),
    # -- sources ------------------------------------------------------------
    'source_range': lambda c: case(_data().range(5).map(identity)),
    'source_lance': lambda c: case(c.lance()),
    'source_lance_columns': lambda c: case(c.lance(columns=['k'])),
    'source_json': lambda c: case(_data().read_json(c.path('rows.jsonl'))),
    'source_csv': lambda c: case(_data().read_csv(c.path('rows.csv'))),
    'source_csv_options': lambda c: case(_data().read_csv(
        c.path('rows.csv'), read_options=__import__('pyarrow.csv', fromlist=['x']).ReadOptions(block_size=4096))),
    'source_sql': lambda c: case(_data().read_sql('select 1', connection)),
    'source_iter': lambda c: case(_data().from_iter(rows_factory)),
    'source_datasource': lambda c: case(_data().read_datasource(FixedDatasource())),
    'source_queue': lambda c: case(_data().read_queue(c.queue()), kind='run_stream'),
    'source_queue_records': lambda c: case(_data().read_queue_records(c.queue(), pool='default', through=c.snapshot())),
    'source_arrow': lambda c: case(_data().from_arrow(__import__('pyarrow').table({'x': [1, 2]}))),
    # -- relational sources -------------------------------------------------
    'join': lambda c: case(c.rows().join(c.rows(), on='k')),
    'join_left': lambda c: case(c.rows().join(c.rows(), on='k', how='left')),
    'join_twice_reduce': lambda c: case(c.rows().join(c.rows(), on='k').join(c.rows(), on='k').reduce_by_key('k', fold)),
    'union': lambda c: case(c.rows().union(c.rows())),
    'union_three': lambda c: case(c.rows().union(c.rows(), c.rows())),
    'union_over_partitions': lambda c: case(c.rows().union(c.rows(), c.rows()), bounds='partitions2'),
    'reduce_by_key': lambda c: case(c.rows().reduce_by_key('k', fold)),
    'reduce_by_key_initial': lambda c: case(c.rows().reduce_by_key('k', fold, initial={'n': 0}, fn_kwargs={})),
    'group_batches': lambda c: case(c.rows().group_batches('k', max_rows=2)),
    'group_batches_streaming': lambda c: case(c.rows().group_batches('k', max_rows=2, flush_interval=1.0)),
    'exclude_keys': lambda c: case(c.lance().exclude_keys(c.lance(), on='k')),
    # -- model operations ---------------------------------------------------
    'map_prompt': lambda c: case(c.rows().map_prompt('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer')),
    'map_prompt_resources': lambda c: case(c.rows().map_prompt(
        'enrich', config=c.pack, inputs={'payload': 'item'}, output='answer',
        backend_options=_resources(memory_bytes=32 * 1024**2, cpu=0.25))),
    'map_prompt_async': lambda c: case(
        c.rows().map_prompt_async('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer')),
    'map_prompt_async_options': lambda c: case(c.rows().map_prompt_async(
        'enrich', config=c.pack, inputs={'payload': 'item'}, output='answer', concurrency=2, catch=(ValueError,),
        call_output='call', error_output='error', label='named')),
    'map_prompt_service': lambda c: case(c.rows().map_prompt_async(
        'enrich', config=c.service_pack(), inputs={'payload': 'item'}, output='answer', service=c.service('mock-model'), concurrency=4,
        options={'trust_env': False, 'timeout_s': 5, 'max_response_bytes': 65536, 'max_request_bytes': 65536,
                 'max_event_bytes': 65536}, error_output='error', call_output='call')),
    'map_prompt_routed': lambda c: case(_routed_prompt(c)),
    'map_prompt_routed_isolated': lambda c: case(_routed_prompt(c, isolate_route_failures=True)),
    'agentmap_async': lambda c: case(c.rows().agentmap_async('enrich', config=c.agent(), inputs={'payload': 'item'},
                                                             output='answer')),
    'map_embeddings': lambda c: case(c.rows().map_embeddings(model=c.model, inputs={'text': 'text'})),
    'map_embeddings_options': lambda c: case(c.rows().map_embeddings(
        model=c.model, inputs={'text': 'text'}, batch_size=4, concurrency=2, call_output='call', error_output='error')),
    'map_embeddings_service': lambda c: case(c.rows().map_embeddings(
        model=__import__('demiflow.embeddings', fromlist=['x']).EmbeddingModel('fixture', 'fixed', 3, 'http://127.0.0.1:59999/v1'),
        inputs={'text': 'text'}, service=c.service())),
    'document_embeddings': lambda c: case(c.rows([{'document_input': 'hi'}]).document_embeddings(model=c.model)),
    'pack_prompt_inputs': lambda c: case(c.rows([{'items': [{'x': 1}]}]).pack_prompt_inputs(
        'enrich', config=c.path('prompt.yaml'), inputs={'payload': 'items'}, item_argument='payload', output='packed', max_items=4,
        max_input_chars=4096, max_request_bytes=65536, max_expansion_bytes=65536)),
    # -- SDK native entries (typed operation + passive factory) -------------
    'map_cached': lambda c: case(c.rows().map_cached(identity, cache_dir=c.path('cache'), version='v1')),
    'map_cached_sync': lambda c: case(c.rows().map_cached(identity, cache_dir=c.path('cache'), version='v1', synchronous=True)),
    'map_cached_stateful': lambda c: case(c.rows().map_cached(Counter(), cache_dir=c.path('cache'), version='v1')),
    'map_cached_async': lambda c: case(c.rows().map_cached(identity_async, cache_dir=c.path('cache'), version='v1')),
    'lookup': lambda c: case(c.rows().lookup({'a': 1}, key='k')),
    'lookup_lance': lambda c: case(c.rows().lookup_lance({'uri': c.lance_uri, 'version': 1}, on='k')),
    'search_vectors': lambda c: case(c.rows([{'vec': [1.0, 2.0]}]).search_vectors(
        query='vec', output='hits', uri=c.lance_uri, vector_column='vec', columns=['x'])),
    'search_web': lambda c: case(c.rows([{'queries': [{'query': 'oak'}]}]).search_web(
        requests='queries', output='found', session=c.web(), request_concurrency=1, concurrency=1, queue_depth=1)),
    'search_web_foreign_session': lambda c: case(c.rows([{'request': 'r'}]).search_web(
        requests='request', output='o', session=Session())),
    'fetch': lambda c: case(c.rows().fetch(store=c.store())),
    'enqueue': lambda c: case(c.rows().enqueue(c.queue(), producer=c.producer()), kind='run_stream'),
    'ack_queue': lambda c: case(c.rows().ack_queue(c.queue())),
    'ack_queue_drop_row': lambda c: case(c.rows().ack_queue(c.queue(), keep_row=False)),
    'admit_rows': lambda c: case(c.rows().admit_rows(path=c.path('admit'), key='k')),
    'deduplicate': lambda c: case(c.rows().deduplicate('k', path=c.path('dedup'), key='k')),
    'prepend': lambda c: case(c.rows().prepend(c.rows([{'x': 0}]), max_rows=10)),
    'save_lance': lambda c: case(c.rows().save_lance(c.path('saved.lance'), schema=c.schema(), key='k', stage='s')),
    'save_lance_when': lambda c: case(c.rows().save_lance(c.path('saved.lance'), schema=c.schema(), key='k', stage='s',
                                                          when=keep)),
    'register_documents': lambda c: case(c.rows([{'request': 'r'}]).register_documents(
        request='request', output='o', library=c.library())),
    'register_documents_batch': lambda c: case(c.rows([{'request': 'r'}]).register_documents(
        request='request', output='o', library=c.library(), batch_size=4)),
    'read_documents': lambda c: case(c.rows([{'request': 'r'}]).read_documents(
        request='request', output='o', context=c.reading())),
    'read_object': lambda c: case(c.rows().read_object(reference='ref', output='o', root=c.path('objroot'))),
    'decode_document': lambda c: case(c.rows().decode_document(column='x', output='o')),
    'encode_image': lambda c: case(c.rows().encode_image(column='x', output='o', profile='thumbnail_lanczos_v1'),
                                 bounds='image_4g'),
    'parse_wikitext': lambda c: case(c.rows().parse_wikitext(column='x', output='o')),
    # -- worker resources ---------------------------------------------------
    'resources_memory_cpu': lambda c: case(c.rows().map(identity, backend_options=_resources(memory_bytes=512 * 1024**2, cpu=0.5))),
    'resources_gpu': lambda c: case(c.rows().map(identity, backend_options=_resources(gpu=1))),
    'resources_gpu_fraction': lambda c: case(c.rows().map(identity, backend_options=_resources(gpu=0.5))),
    'resources_custom': lambda c: case(c.rows().map(identity, backend_options=_resources(custom_resources={'license': 1.5}))),
    'resources_memory_too_small': lambda c: case(c.rows().map(identity, backend_options=_resources(memory_bytes=1024))),
    'resources_compute_pool': lambda c: case(c.rows().map(identity, backend_options={'ray': {'compute': {'kind': 'task_pool', 'size': 2}}})),
    # -- terminals ----------------------------------------------------------
    'terminal_take_all': lambda c: case(c.rows().map(identity), 'take_all', {'limit': None}),
    'terminal_take_zero': lambda c: case(c.rows().map(identity), 'take', {'limit': 0}),
    'terminal_take_batch': lambda c: case(c.rows().map(identity), 'take_batch', {'batch_size': 2, 'batch_format': 'default'}),
    'terminal_take_batch_zero': lambda c: case(c.rows().map(identity), 'take_batch', {'batch_size': 0, 'batch_format': 'default'}),
    'terminal_iter_rows': lambda c: case(c.rows().map(identity), 'iter_rows'),
    'terminal_iter_batches': lambda c: case(c.rows().map(identity), 'iter_batches', {
        'prefetch_batches': 1, 'batch_size': 256, 'batch_format': 'default', 'drop_last': False}),
    'terminal_count': lambda c: case(c.rows().map(identity), 'count'),
    'terminal_aggregate': lambda c: case(c.rows().map(identity), 'aggregate', {'names': ['count()', 'sum(x)']},
                                         programs=_aggregates()),
    'terminal_schema': lambda c: case(c.rows().map(identity), 'schema', {'fetch_if_missing': True}),
    'terminal_columns': lambda c: case(c.rows().map(identity), 'columns', {'fetch_if_missing': True}),
    'terminal_size_bytes': lambda c: case(c.rows().map(identity), 'size_bytes'),
    'terminal_materialize': lambda c: case(c.rows().map(identity), 'materialize'),
    'terminal_collect_summary': lambda c: case(c.rows().map(identity), 'collect_summary'),
    'terminal_run_stream': lambda c: case(c.rows().map_async(identity_async, concurrency=1), 'run_stream'),
    'terminal_write_datasink': lambda c: case(c.rows().map(identity), 'write_datasink', sink=FixedSink()),
    'terminal_write_datasink_missing': lambda c: case(c.rows().map(identity), 'write_datasink'),
    'terminal_write_datasink_queue': lambda c: case(c.rows().map(identity), 'write_datasink', sink=_queue_sink(c)),
    'terminal_write_lance': lambda c: case(c.rows().map(identity), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'terminal_write_lance_sorted': lambda c: case(c.rows().sort('x'), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'terminal_write_lance_join': lambda c: case(c.rows().join(c.rows(), on='k'), 'write_lance',
                                                {'uri': c.path('written.lance'), 'mode': 'create'}),
    'terminal_write_file': lambda c: case(c.rows().map(identity), 'write_file',
                                          {'format': 'parquet', 'path': c.path('out.parquet'), 'options': {}}),
    'terminal_unknown': lambda c: case(c.rows().map(identity), 'not_a_terminal'),
    # -- routed graphs --------------------------------------------------------
    'graph_broadcast': lambda c: _graph(c, 'broadcast'),
    'graph_rebalance': lambda c: _graph(c, 'rebalance'),
    'graph_hash': lambda c: _graph(c, 'hash', key_fields=['k']),
    'graph_target': lambda c: _graph(c, 'target', target_field='k', target_classes={'a': ['first'], 'b': ['second']}),
    'graph_durable': lambda c: _graph(c, 'broadcast', exchange_mode='durable_incremental', storage_bytes=1 << 20,
                                      credit_budget={'rows': 8, 'bytes': 65536}, prefetch_slots=2, cut_rows=4, queue_depth=4,
                                      source_id='src'),
    'graph_without_ancestor': lambda c: _graph(c, 'broadcast', ancestor=False),
    'graph_actor_target': lambda c: _graph(c, 'broadcast', actor_target=True),
    'graph_literal_prefix': lambda c: _graph(c, 'broadcast', base=_data().range(5)),
    'graph_literal_prefix_target': lambda c: _graph(c, 'rebalance', base=_data().range(5), actor_target=True),
    # -- structural budgets (refusals) --------------------------------------
    'refuse_nodes': lambda c: case(c.rows().map(identity).map(identity).map(identity), bounds='nodes3'),
    'refuse_depth': lambda c: case(c.rows().join(c.rows().join(c.rows(), on='k'), on='k'), bounds='depth1'),
}

# A native declaration placed in a plan directly, for each registered native name. The public
# entries never do this: they preflight the name and append a typed operation.
def _direct_native(c, name):
    from demiflow.data.plan import LogicalPlan
    from demiflow.execution.executors.dataflow_api import make_native_operation
    base = c.rows()
    return dict(dataset=None, source=base._source, plan=LogicalPlan((make_native_operation(name, {}, bindings={}),)),
                kind='take', arguments={'limit': 1})


NATIVE_NAMES = ('prompt', 'embedding', 'agent', 'raw_fetch', 'queue_produce', 'queue_ack', 'lance_lookup',
                'search_vectors', 'search_web', 'deduplicate', 'group_batches', 'stream_prepend', 'save_lance',
                'register_documents', 'register_documents_batch', 'read_documents', 'pack_prompt_inputs',
                'read_local_object', 'decode_document', 'encode_image', 'parse_wikitext')
for _name in NATIVE_NAMES:
    CASES['native/' + _name] = lambda c, name=_name: _direct_native(c, name)

# Executor defaults: the same plans with and without each stand-in executor.
for _name, _plan in {
    'map_batches': lambda c: c.rows().map_batches(batch),
    'map_batches_sized': lambda c: c.rows().map_batches(batch, batch_size=4),
    'random_shuffle': lambda c: c.rows().random_shuffle(seed=1),
    'random_shuffle_blocks': lambda c: c.rows().random_shuffle(seed=1, num_blocks=2),
    'task_chain': lambda c: c.rows().map(identity).select_columns(['x']).filter(keep).flat_map(many),
    'streaming_map': lambda c: c.rows().map(identity, concurrency=2),
}.items():
    for _executor in EXECUTORS:
        CASES['executor/' + _name + '@' + _executor] = (
            lambda c, plan=_plan, executor=_executor: case(plan(c), executor=executor))

# Report context: the same plans with a report context that binds every object.
for _name, _build in {
    'chain': lambda c: case(c.rows().map(identity).filter(keep).map(identity)),
    'chain_terminal_unbound': lambda c: case(c.rows().map(identity).filter(keep), report_terminal=False),
    'shared_prefix_union': lambda c: (lambda base: case(base.map(identity).union(base.filter(keep))))(c.rows().map(identity)),
    'join': lambda c: case(c.rows().map(identity).join(c.rows(), on='k')),
    'prepend': lambda c: case(c.rows().prepend(c.rows([{'x': 0}]), max_rows=10)),
    'exclude_keys': lambda c: case(c.lance().exclude_keys(c.lance(), on='k')),
    'take_zero': lambda c: case(c.rows().map(identity), 'take', {'limit': 0}),
    'write_lance': lambda c: case(c.rows().map(identity), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'graph_broadcast': lambda c: _graph(c, 'broadcast'),
    'graph_target': lambda c: _graph(c, 'target', target_field='k', target_classes={'a': ['first'], 'b': ['second']}),
    'graph_literal_prefix': lambda c: _graph(c, 'broadcast', base=_data().range(5)),
}.items():
    CASES['report/' + _name] = lambda c, build=_build: dict(build(c), report=True)

# Memory budgets: budget-sensitive plans across the bounds used by the budget tests.
_BUDGET_PLANS = {
    'items': lambda c: case(c.rows(), 'take_all', {'limit': None}),
    'map_chain': lambda c: case(c.rows().map(identity).map(identity).filter(keep), 'take_all', {'limit': None}),
    'stateful': lambda c: case(c.rows().map(Counter()).map_batches(batch), 'take_all', {'limit': None}),
    'sorts': lambda c: case(_data().from_iter(rows_factory).limit(2).sort('n').sort('n'), 'take_all', {'limit': None}),
    'worker_memory': lambda c: case(c.rows().map(identity, backend_options=_resources(memory_bytes=128 * 1024)),
                                    'take_all', {'limit': None}),
    'join': lambda c: case(c.rows().join(c.rows(), on='k'), 'take_all', {'limit': None}),
    'reduce': lambda c: case(c.rows().reduce_by_key('k', fold, initial={'n': 0}), 'take_all', {'limit': None}),
    'groups': lambda c: case(c.rows().group_batches('k', max_rows=2), 'take_all', {'limit': None}),
    'prompt': lambda c: case(c.rows().map_prompt('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer'),
                             'take_all', {'limit': None}),
    'prompt_async': lambda c: case(c.rows().map_prompt_async('enrich', config=c.pack, inputs={'payload': 'item'},
                                                             output='answer', concurrency=2), 'take_all', {'limit': None}),
    'embeddings': lambda c: case(c.rows().map_embeddings(model=c.model, inputs={'text': 'text'}), 'take_all', {'limit': None}),
    'fetch': lambda c: case(c.rows().fetch(store=c.store()), 'take_all', {'limit': None}),
    'write_lance': lambda c: case(c.rows().map(identity), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_lance_items': lambda c: case(c.rows(), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_lance_sorted': lambda c: case(c.rows().sort('x'), 'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_lance_join': lambda c: case(c.rows().join(c.rows(), on='k'), 'write_lance',
                                       {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_lance_two_joins': lambda c: case(c.rows().join(c.rows(), on='k').join(c.rows(), on='k'), 'write_lance',
                                            {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_lance_prompt': lambda c: case(c.rows().map_prompt('enrich', config=c.pack, inputs={'payload': 'item'},
                                                             output='answer'),
                                         'write_lance', {'uri': c.path('written.lance'), 'mode': 'create'}),
    'write_file': lambda c: case(c.rows().map(identity), 'write_file',
                                 {'format': 'parquet', 'path': c.path('out.parquet'), 'options': {}}),
    'write_datasink': lambda c: case(c.rows().map(identity), 'write_datasink', sink=FixedSink()),
    'write_datasink_queue': lambda c: case(c.rows().map(identity), 'write_datasink', sink=_queue_sink(c)),
    'graph': lambda c: _graph(c, 'broadcast'),
    'graph_literal_prefix': lambda c: _graph(c, 'broadcast', base=_data().range(5)),
}
_BUDGET_BOUNDS = ('m192_r8_b32', 'm256_r16_b32', 'm64_r8_b20', 'm80_r8_b20', 'm80_r8_b16_s128', 'm192_state3071',
                  'row4k_batch8k_state16k')
for _name, _build in _BUDGET_PLANS.items():
    for _bounds in _BUDGET_BOUNDS:
        CASES['budget/' + _name + '@' + _bounds] = lambda c, build=_build, limits=_bounds: dict(build(c), bounds=limits)


# The real terminal methods under the production capture entry.
CAPTURED = {
    'take': lambda c: c.rows().map(identity).take(2),
    'take_zero': lambda c: c.rows().map(identity).take(0),
    'take_all': lambda c: c.rows().map(identity).take_all(),
    'take_batch': lambda c: c.rows().map(identity).take_batch(2),
    'count': lambda c: c.rows().map(identity).count(),
    'sum': lambda c: c.rows().map(identity).sum('x'),
    'aggregate': lambda c: c.rows().map(identity).aggregate(
        __import__('demiflow.data.aggregate', fromlist=['x']).Count(), __import__('demiflow.data.aggregate', fromlist=['x']).Sum('x')),
    'schema': lambda c: c.rows().map(identity).schema(),
    'columns': lambda c: c.rows().map(identity).columns(),
    'size_bytes': lambda c: c.rows().map(identity).size_bytes(),
    'materialize': lambda c: c.rows().map(identity).materialize(),
    'iter_rows': lambda c: list(c.rows().map(identity).iter_rows()),
    'iter_batches': lambda c: list(c.rows().map(identity).iter_batches()),
    'collect_summary': lambda c: c.rows().map(identity).collect_summary(max_rows=4),
    'run_stream': lambda c: c.rows().map_async(identity_async, concurrency=1).run_stream(),
    'write_datasink': lambda c: c.rows().map(identity).write_datasink(FixedSink()),
    'write_lance': lambda c: c.rows().map(identity).write_lance(c.path('captured.lance'), mode='create'),
    'write_parquet': lambda c: c.rows().map(identity).write_parquet(c.path('captured.parquet')),
    'write_json': lambda c: c.rows().map(identity).write_json(c.path('captured.json')),
    'write_csv': lambda c: c.rows().map(identity).write_csv(c.path('captured.csv')),
    'map_batches': lambda c: c.rows().map_batches(batch).take(2),
    'random_shuffle': lambda c: c.rows().random_shuffle(seed=1).take(2),
    'task_chain': lambda c: c.rows().map(identity).select_columns(['x']).filter(keep).take(2),
    'map_prompt': lambda c: c.rows().map_prompt('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer').take(2),
    'map_prompt_async': lambda c: c.rows().map_prompt_async('enrich', config=c.pack, inputs={'payload': 'item'},
                                                            output='answer').run_stream(),
    'map_embeddings': lambda c: c.rows().map_embeddings(model=c.model, inputs={'text': 'text'}).take(2),
    'join': lambda c: c.rows().join(c.rows(), on='k').take(2),
    'lance_exclude_keys': lambda c: c.lance().exclude_keys(c.lance(), on='k').take(2),
    'save_lance': lambda c: c.rows().save_lance(c.path('captured-saved.lance'), schema=c.schema(), key='k', stage='s').run_stream(),
    'fetch': lambda c: c.rows().fetch(store=c.store()).run_stream(),
}


# -- compiling and projecting -----------------------------------------------------
def _describe(value):
    from demiflow.execution.seal.program_identity import describe
    try:
        return describe(value)
    except Exception as exc:
        return {'undescribed': type(value).__module__ + '.' + type(value).__qualname__, 'reason': type(exc).__name__}


def _plain(value):
    """Strict JSON: mappings and sequences only, objects by type name (never by address)."""
    if isinstance(value, dict) or hasattr(value, 'items'):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    return {'object': type(value).__module__ + '.' + type(value).__qualname__}


def _declaration(declaration):
    """One program declaration in full: what the worker would be handed for this occurrence."""
    out = {'occurrence_id': declaration.occurrence_id, 'code_identity': _plain(declaration.code_identity)}
    spec = declaration.callable_spec
    if spec is not None:
        out['callable'] = {'spec': _plain(_describe(spec)), 'name': getattr(spec, 'name', None),
                           'runtime_contract': _plain(_describe(getattr(spec, 'runtime_contract', None)))}
    if declaration.source_spec is not None:
        source = declaration.source_spec
        out['source'] = {'type': type(source).__module__ + '.' + type(source).__qualname__,
                         'declaration': _plain(_describe(source))}
    program = declaration.action_program
    if program is not None:
        out['program'] = {'name': program.name, 'kind': program.kind, 'value': _plain(_describe(program.value))}
    return out


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def _summary(compiled, limits, source, plan, raw=None):
    from demiflow.execution.contracts.dataflow_codec import encode_wire
    graph = compiled.graph
    encoded = encode_wire(graph, type_name='DataflowGraph', bounds=limits)
    if raw is not None:
        raw.write_bytes(encoded)
    nodes = []
    for node in graph.nodes:
        kernel = node.kernel
        nodes.append({
            'node_kind': node.kind, 'kernel_kind': kernel.kind, 'state': kernel.state, 'effect': kernel.effect,
            'resource_role': kernel.resource_role, 'mergeable': kernel.mergeable,
            'inputs': [[port.name, port.requirement, port.ordering] for port in kernel.inputs],
            'requests': _plain(kernel.profile.requests),
            'configuration': {key: type(value).__name__ for key, value in sorted(kernel.configuration.items())},
        })
        if 'native_operation' in kernel.configuration:
            nodes[-1]['native_operation'] = kernel.configuration['native_operation']
    declarations = [_declaration(declaration) for declaration in compiled.declarations]
    labels = sorted([occurrence, label] for occurrence, label in compiled.observation_labels.items())
    bindings = [_plain(binding) for binding in compiled.report_bindings]
    return {'ok': True, 'sha256': hashlib.sha256(encoded).hexdigest(), 'encoded_bytes': len(encoded),
            'dag_id': graph.dag_id, 'nodes': nodes,
            'declarations': declarations, 'declarations_sha256': _digest(declarations),
            'declaration_kinds': sorted('callable' if 'callable' in d else 'source' if 'source' in d else 'action'
                                        for d in declarations),
            'source': type(source).__name__,
            'operations': [type(operation).__name__ for operation in plan.operations],
            'report_bindings': bindings, 'observation_labels': labels}


def _refusal(exc):
    code = getattr(exc, 'code', None)
    return {'ok': False, 'error': type(exc).__name__ + (':' + code if isinstance(code, str) else '') + ': ' + str(exc)[:300]}


def lower_case(name, context):
    """Build and lower one direct case; returns (compiled, bounds, source, plan) or raises the refusal."""
    from demiflow.execution.pipeline import checked
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.planning.dataflow_compile import lower_action
    token = None
    try:
        built = CASES[name](context)
        dataset = built.get('dataset')
        source = built['source'] if dataset is None else dataset._source
        plan = built['plan'] if dataset is None else dataset._plan
        limits = BOUNDS[built.get('bounds', 'default')]()
        action_id = 'corpus-' + name.replace('/', '-').replace('@', '-')
        action = type('Action', (), {'action_id': action_id})()
        if built.get('executor') is not None:
            action._dataflow_executor = EXECUTORS[built['executor']]()
        request = ActionRequest(kind=built['kind'], source=source, plan=plan, arguments=built['arguments'],
                                sink=built.get('sink'), programs=tuple(built.get('programs', ())), provider='local',
                                action=action)
        if built.get('report'):
            token = checked._CURRENT.set(report_for(source, plan, action_id, terminal=built.get('report_terminal', True)))
        return lower_action(request, bounds=limits), limits, source, plan
    finally:
        if token is not None:
            checked._CURRENT.reset(token)


def compile_case(name, context, raw=None):
    try:
        compiled, limits, source, plan = lower_case(name, context)
        return _summary(compiled, limits, source, plan, raw)
    except Exception as exc:  # recorded: a refused declaration is part of the contract
        return _refusal(exc)


def capture(name, context):
    """The real terminal method, stopped at the production planning boundary."""
    from demiflow.execution.pipeline.managed_native import capture_pipeline
    return capture_pipeline(lambda: CAPTURED[name](context), {})


def capture_case(name, context, raw=None):
    from dataclasses import asdict
    try:
        captured = capture(name, context)
    except Exception as exc:
        return _refusal(exc)
    graph = captured.compiled.graph
    summary = _summary(captured.compiled, captured.bounds, type('S', (), {})(), type('P', (), {'operations': ()})(), raw)
    summary.pop('source'), summary.pop('operations')
    summary.update(kind=captured.kind, provider=captured.provider, action_id=graph.action_id,
                   bounds=_plain(asdict(captured.bounds)), policy=_plain(_describe(captured.policy)))
    return summary


def dump(root, names=None, raw=None):
    import demiflow
    context = Context(root)
    if raw is not None:
        Path(raw).mkdir(parents=True, exist_ok=True)
    target = lambda family, name: None if raw is None else Path(raw) / (family + '-' + name.replace('/', '-') + '.wire')
    return {'engine': str(Path(demiflow.__file__).resolve().parents[1]), 'root': str(context.root),
            'cases': {name: compile_case(name, context, target('case', name)) for name in sorted(names or CASES)},
            'captured': {name: capture_case(name, context, target('captured', name)) for name in sorted(CAPTURED)}}


def structure(result):
    """The committed baseline: shapes only, no identities, digests or machine-dependent sizes."""
    cases = {}
    for name, item in result['cases'].items():
        if not item['ok']:
            cases[name] = {'ok': False, 'error': item['error'].split(': ', 1)[0]}
            continue
        cases[name] = {'ok': True, 'declarations': item['declaration_kinds'], 'source': item['source'],
                       'operations': item['operations'], 'report_bindings': [
                           [binding['report_node_id'], binding['report_occurrence'], binding['mode']]
                           for binding in item['report_bindings']],
                       'observation_labels': sorted(label for _, label in item['observation_labels']),
                       'nodes': [{key: value for key, value in node.items() if key != 'requests'}
                                 for node in item['nodes']]}
    return cases


def render_structure(cases):
    """One case per line, so a change to one declaration is one changed line."""
    lines = [json.dumps(name) + ': ' + json.dumps(cases[name], sort_keys=True, separators=(',', ':'))
             for name in sorted(cases)]
    return '{\n' + ',\n'.join(lines) + '\n}\n'


def compare(left, right):
    """Every difference between two dumps, field by field. Empty means byte-identical outputs."""
    differences = []
    for family in ('cases', 'captured'):
        a, b = left[family], right[family]
        for name in sorted(set(a) | set(b)):
            if name not in a or name not in b:
                differences.append((family, name, 'present only in ' + ('left' if name in a else 'right')))
                continue
            for field in sorted(set(a[name]) | set(b[name])):
                if a[name].get(field) != b[name].get(field):
                    differences.append((family, name, field))
    return differences


if __name__ == '__main__':
    if sys.argv[1] == 'compare':
        left, right = (json.loads(Path(path).read_text(encoding='utf-8')) for path in sys.argv[2:4])
        found = compare(left, right)
        total = len(left['cases']) + len(left['captured'])
        print('left ', left['engine'])
        print('right', right['engine'])
        print(total, 'cases,', len({(family, name) for family, name, _ in found}), 'differ')
        for family, name, field in found[:80]:
            print('  ', family, name, field)
        sys.exit(1 if found else 0)
    result = dump(sys.argv[1], raw=sys.argv[4] if len(sys.argv) > 4 else None)
    Path(sys.argv[2]).write_text(json.dumps(result, indent=1, sort_keys=True) + '\n', encoding='utf-8')
    if len(sys.argv) > 3 and sys.argv[3] != '-':   # also write the identity-free structural baseline
        Path(sys.argv[3]).write_text(render_structure(structure(result)), encoding='utf-8')
    for family in ('cases', 'captured'):
        refused = {name: item['error'] for name, item in result[family].items() if not item['ok']}
        print(family + ':', len(result[family]), 'cases,', len(refused), 'refused')
        for name, error in sorted(refused.items()):
            print('  ', name, '->', error[:230])
