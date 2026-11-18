"""Compile corpus for the compile <-> runtime contract work.

Each case builds a plan through the real declaration path (the public Dataset
entries, with the same arguments the entry tests use) and lowers it with the
real compiler. Nothing is executed: no source is read, no actor is started.

Two uses:

* ``dump(root)`` compiles every case and returns, per case, the SHA-256 of the
  fully encoded graph (every KernelSpec field, not only what ``dag_id`` covers)
  and a readable summary. Running it once with the baseline engine tree and
  once with a changed tree, against the same ``root``, shows byte for byte
  whether a compiler change altered any output. Nothing is normalised.
* the summary without identities is the committed structural baseline.

The functions used by the cases live in this module and must not be edited
casually: a function's identity includes the hash of its whole source file.
"""
import hashlib
import json
import sys
from pathlib import Path

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


def identity(row):
    return row


def keep(row):
    return True


def many(row):
    return [row, row]


def batch(rows):
    return rows


async def identity_async(row):
    return row


def fold(state, row):
    return state


class Counter:
    def __init__(self):
        self.seen = 0

    def __call__(self, row):
        self.seen += 1
        return row


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

    def rows(self, rows=None):
        from demiflow import data
        return data.from_items(ROWS if rows is None else rows)

    def path(self, name):
        return str(self.root / name)


def _take(dataset):
    return dataset, 'take', {'limit': 2}


CASES = {
    # -- typed operations ---------------------------------------------------
    'map': lambda c: _take(c.rows().map(identity)),
    'flat_map': lambda c: _take(c.rows().flat_map(many)),
    'filter': lambda c: _take(c.rows().filter(keep)),
    'map_batches': lambda c: _take(c.rows().map_batches(batch)),
    'map_stateful_instance': lambda c: _take(c.rows().map(Counter())),
    'map_class_factory': lambda c: _take(c.rows().map(Counter)),
    'map_async': lambda c: _take(c.rows().map_async(identity_async, concurrency=2)),
    'select_drop_rename': lambda c: _take(c.rows().select_columns(['x', 'k']).rename_columns({'x': 'y'})),
    'add_column': lambda c: _take(c.rows().add_column('z', identity)),
    'drop_columns': lambda c: _take(c.rows().drop_columns(['text'])),
    'limit': lambda c: _take(c.rows().limit(1)),
    'random_sample': lambda c: _take(c.rows().random_sample(0.5, seed=1)),
    'sort': lambda c: _take(c.rows().sort('x')),
    'repartition': lambda c: _take(c.rows().repartition(num_blocks=2)),
    'random_shuffle': lambda c: _take(c.rows().random_shuffle(seed=1)),
    'randomize_block_order': lambda c: _take(c.rows().randomize_block_order(seed=1)),
    'chain_map_filter_map': lambda c: _take(c.rows().map(identity).filter(keep).map(identity)),
    # -- relational sources -------------------------------------------------
    'join': lambda c: _take(c.rows().join(c.rows(), on='k')),
    'union': lambda c: _take(c.rows().union(c.rows())),
    'reduce_by_key': lambda c: _take(c.rows().reduce_by_key('k', fold)),
    'group_batches': lambda c: _take(c.rows().group_batches('k', max_rows=2)),
    'exclude_keys': lambda c: _take(
        __import__('demiflow').data.read_lance(c.lance_uri).exclude_keys(
            __import__('demiflow').data.read_lance(c.lance_uri), on='k')),
    'read_lance': lambda c: _take(__import__('demiflow').data.read_lance(c.lance_uri)),
    'range': lambda c: _take(__import__('demiflow').data.range(5).map(identity)),
    # -- model operations ---------------------------------------------------
    'map_prompt': lambda c: _take(c.rows().map_prompt('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer')),
    'map_prompt_async': lambda c: _take(
        c.rows().map_prompt_async('enrich', config=c.pack, inputs={'payload': 'item'}, output='answer')),
    'map_embeddings': lambda c: _take(c.rows().map_embeddings(model=c.model, inputs={'text': 'text'})),
    # -- SDK native entries (typed operation + passive factory) -------------
    'map_cached': lambda c: _take(c.rows().map_cached(identity, cache_dir=c.path('cache'), version='v1')),
    'lookup_lance': lambda c: _take(c.rows().lookup_lance({'uri': c.lance_uri, 'version': 1}, on='k')),
    'fetch': lambda c: _take(c.rows().fetch(store=__import__('demiflow.objects', fromlist=['x']).LocalObjectStore(c.path('objs')))),
    'prepend': lambda c: _take(c.rows().prepend(c.rows([{'x': 0}]), max_rows=10)),
    'decode_document': lambda c: _take(c.rows().decode_document(column='x', output='o')),
    'parse_wikitext': lambda c: _take(c.rows().parse_wikitext(column='x', output='o')),
    # -- terminals ------------------------------------------------------------
    'terminal_take_all': lambda c: (c.rows().map(identity), 'take_all', {}),
    'terminal_count': lambda c: (c.rows().map(identity), 'count', {}),
    'terminal_take_zero': lambda c: (c.rows().map(identity), 'take', {'limit': 0}),
    'terminal_materialize': lambda c: (c.rows().map(identity), 'materialize', {}),
    'terminal_iter_rows': lambda c: (c.rows().map(identity), 'iter_rows', {}),
}


def bounds():
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    return resolve_runtime_policy({}, {}, provider='local')[1]


def _summary(compiled, limits, dataset=None):
    from demiflow.execution.contracts.dataflow_codec import encode_wire
    graph = compiled.graph
    encoded = encode_wire(graph, type_name='DataflowGraph', bounds=limits)
    nodes = []
    for node in graph.nodes:
        kernel = node.kernel
        nodes.append({
            'node_kind': node.kind, 'kernel_kind': kernel.kind, 'state': kernel.state, 'effect': kernel.effect,
            'resource_role': kernel.resource_role, 'mergeable': kernel.mergeable,
            'inputs': [[port.name, port.requirement, port.ordering] for port in kernel.inputs],
            'requests': {key: kernel.profile.requests[key] for key in sorted(kernel.profile.requests)},
            'configuration': {key: type(value).__name__ for key, value in sorted(kernel.configuration.items())},
        })
    declarations = sorted(
        ('callable' if d.callable_spec is not None else 'source' if d.source_spec is not None else 'action')
        for d in compiled.declarations)
    return {'ok': True, 'sha256': hashlib.sha256(encoded).hexdigest(), 'encoded_bytes': len(encoded),
            'dag_id': graph.dag_id, 'nodes': nodes, 'declarations': declarations,
            'source': type(dataset._source).__name__,
            'operations': [type(operation).__name__ for operation in dataset._plan.operations],
            'report_bindings': len(compiled.report_bindings),
            'observation_labels': sorted(compiled.observation_labels.values())}


def compile_case(name, context, limits):
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.planning.dataflow_compile import lower_action
    try:
        dataset, kind, arguments = CASES[name](context)
        request = ActionRequest(kind=kind, source=dataset._source, plan=dataset._plan, arguments=arguments,
                                sink=None, programs=(), provider='local',
                                action=type('Action', (), {'action_id': 'corpus-' + name})())
        return _summary(lower_action(request, bounds=limits), limits, dataset)
    except Exception as exc:  # recorded: a refused declaration is part of the contract
        return {'ok': False, 'error': type(exc).__name__ + ': ' + str(exc)[:300]}


def dump(root, names=None):
    import demiflow
    context, limits = Context(root), bounds()
    return {'engine': str(Path(demiflow.__file__).resolve().parents[1]), 'root': str(context.root),
            'cases': {name: compile_case(name, context, limits) for name in sorted(names or CASES)}}


def structure(result):
    """The committed baseline: shapes only, no identities or digests."""
    cases = {}
    for name, case in result['cases'].items():
        if not case['ok']:
            cases[name] = {'ok': False, 'error': case['error'].split(':', 1)[0]}
            continue
        cases[name] = {'ok': True, 'declarations': case['declarations'], 'source': case['source'],
                       'operations': case['operations'], 'nodes': [
            {key: value for key, value in node.items() if key != 'requests'} for node in case['nodes']]}
    return cases


if __name__ == '__main__':
    result = dump(sys.argv[1])
    Path(sys.argv[2]).write_text(json.dumps(result, indent=1, sort_keys=True) + '\n', encoding='utf-8')
    if len(sys.argv) > 3:   # also write the identity-free structural baseline
        Path(sys.argv[3]).write_text(json.dumps(structure(result), indent=1, sort_keys=True) + '\n', encoding='utf-8')
    failed = {name: case['error'] for name, case in result['cases'].items() if not case['ok']}
    print(len(result['cases']), 'cases,', len(failed), 'refused')
    for name, error in sorted(failed.items()):
        print('  ', name, '->', error[:200])
