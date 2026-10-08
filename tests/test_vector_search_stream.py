"""Native per-row retrieval, snapshot, resource and failure contracts."""
import asyncio
import threading
import time

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.plan import VectorSearchOp
from demiflow.errors import InvalidLanceRequest
from demiflow.lance import search


@pytest.fixture
def table(tmp_path):
    uri = str(tmp_path / 'vectors.lance')
    schema = pa.schema([('id', pa.string()), ('group', pa.string()),
                        ('embedding', pa.list_(pa.float32(), 2)), ('text', pa.string())])
    rows = [dict(id='a', group='x', embedding=[1., 0.], text='A'),
            dict(id='b', group='y', embedding=[0., 1.], text='B'),
            dict(id='c', group='x', embedding=[-1., 0.], text='C')]
    lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), uri)
    return uri, schema


def plan(uri, rows, **kwargs):
    settings = dict(query='vector', output='hits', uri=uri,
                    vector_column='embedding', columns=['id'], top_k=1,
                    metric='l2', concurrency=2, queue_depth=1)
    settings.update(kwargs)
    return data.from_items(rows).search_vectors(**settings)


def collect(stream):
    rows = []
    stats = stream.map(lambda row: rows.append(row) or row).run_stream()
    return rows, stats


def test_lazy_native_node_preserves_queries_and_duplicates(table, monkeypatch):
    uri, _ = table
    opened = []
    native_open = search.open_lance_dataset
    def tracked(*args, **kwargs):
        opened.append((args, kwargs))
        return native_open(*args, **kwargs)
    monkeypatch.setattr(search, 'open_lance_dataset', tracked)
    inputs = [dict(qid='one', vector=[1., 0.]), dict(qid='two', vector=[0., 1.]),
              dict(qid='duplicate', vector=[1., 0.])]
    stream = plan(uri, inputs)
    assert isinstance(stream._plan.operations[-1], VectorSearchOp)
    assert not opened
    rows, stats = collect(stream)
    by_id = {r['qid']: r for r in rows}
    assert {k: v['hits'][0]['id'] for k, v in by_id.items()} == {
        'one': 'a', 'two': 'b', 'duplicate': 'a'}
    assert all(set(r['hits'][0]) == {'id', '_distance'} for r in rows)
    assert all(r['hits'][0]['_distance'] == 0 for r in rows)
    assert len(opened) == 1
    assert opened[0][1]['index_cache_size_bytes'] == 256 * 2**20
    assert stats.metrics['resources']['VectorSearch:0']['version'] == 1
    assert stats.metrics['resources']['VectorSearch:0']['queries'] == 3
    assert stream._stages[-1]._dataset is None
    collect(stream)
    assert len(opened) == 2  # Action-owned handle can be reopened safely.


def test_static_and_per_row_filters_empty_hits_and_materialization(table):
    uri, _ = table
    rows = plan(uri, [dict(vector=[0., 1.], predicate="id = 'c'"),
                      dict(vector=[0., 1.], predicate="id = 'b'")],
                filter="group = 'x'", filter_column='predicate').materialize().take_all()
    by_filter = {r['predicate']: r['hits'] for r in rows}
    assert by_filter["id = 'c'"][0]['id'] == 'c'
    assert by_filter["id = 'b'"] == []


def test_head_pinned_for_action_and_explicit_version(table):
    uri, schema = table
    appended = False
    def after_search(row):
        nonlocal appended
        if not appended:
            lance.write_dataset(pa.Table.from_pylist([
                dict(id='new', group='z', embedding=[.5, .5], text='D')], schema=schema), uri, mode='append')
            appended = True
        return row
    inputs = [dict(vector=[.5, .5])] * 6
    rows, stats = collect(plan(uri, inputs, concurrency=1).map(after_search))
    assert all(r['hits'][0]['id'] != 'new' for r in rows)
    assert stats.metrics['resources']['VectorSearch:0']['version'] == 1
    rows, _ = collect(plan(uri, inputs[:1], version=1))
    assert rows[0]['hits'][0]['id'] != 'new'
    rows, _ = collect(plan(uri, inputs[:1]))
    assert rows[0]['hits'][0]['id'] == 'new'


def test_empty_input_does_not_open_table(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('empty stream opened the table')
    monkeypatch.setattr(search, 'open_lance_dataset', forbidden)
    assert collect(plan('/missing.lance', []))[0] == []


@pytest.mark.parametrize('vector', [None, [], [1.], [[1., 0.]], [float('nan'), 0.],
                                   [1e40, 0.], [True, 0.], ['1', '0']])
def test_invalid_queries_fail_instead_of_dropping_rows(table, vector):
    stream = plan(table[0], [dict(vector=vector)])
    with pytest.raises(InvalidLanceRequest):
        stream.run_stream()
    assert stream._stages[-1]._dataset is None


@pytest.mark.parametrize('kwargs', [dict(columns=[]), dict(output='vector'),
    dict(concurrency=True), dict(queue_depth=0), dict(version=0), dict(top_k=0),
    dict(options={'typo': 1}), dict(options={'use_index': 1}),
    dict(options={'nprobes': 0}), dict(columns=['_distance']),
    dict(options={'ef': 0}), dict(options={'ef': True}),
    dict(options={'ef': 1.5}), dict(options={'ef': '200'}),
    dict(options={'max_search_candidates': 0}),
    dict(top_k=100, options={'refine_factor': 5, 'ef': 200})])
def test_invalid_configuration_rejected_at_declaration(kwargs):
    with pytest.raises((ValueError, InvalidLanceRequest)):
        plan('/missing.lance', [], **kwargs)


def test_query_and_result_byte_admission(table, monkeypatch):
    uri, schema = table
    stream = plan(uri, [dict(vector=[1., 0.])], options={'max_query_dimensions': 1})
    with pytest.raises(InvalidLanceRequest, match='max_query_dimensions'):
        stream.run_stream()
    assert stream._stages[-1]._version is None  # Refused before table I/O.
    lance.write_dataset(pa.Table.from_pylist([
        dict(id='wide', group='z', embedding=[1., 0.], text='x' * 20000)], schema=schema), uri, mode='overwrite')
    for options, message in [({'max_result_bytes': 1000}, 'max_result_bytes'),
                              ({'max_python_result_bytes': 2000}, 'max_python_result_bytes')]:
        stream = plan(uri, [dict(vector=[1., 0.])], columns=['id', 'text'], options=options)
        with pytest.raises(MemoryError, match=message):
            stream.run_stream()
        assert stream._stages[-1]._dataset is None
    with pytest.raises(MemoryError, match='top_k'):
        plan(uri, [], top_k=10000, options={'max_python_result_bytes': 1000})


def test_native_index_and_exact_scan_paths(tmp_path, monkeypatch):
    import numpy as np
    vectors = np.random.default_rng(0).normal(size=(256, 8)).astype('float32')
    uri = str(tmp_path / 'indexed.lance')
    lance.write_dataset(pa.table({'id': list(range(256)), 'embedding':
        pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), 8)}), uri)
    indexed = lance.dataset(uri)
    indexed.create_index('embedding', 'IVF_FLAT', num_partitions=2, metric='l2')
    version = indexed.version
    assert version > 1 and indexed.describe_indices()
    plans = []
    native_open = search.open_lance_dataset
    class Watched:
        def __init__(self, ds): self.ds = ds
        def __getattr__(self, name): return getattr(self.ds, name)
        def scanner(self, **kwargs):
            scanner = self.ds.scanner(**kwargs)
            plans.append(scanner.explain_plan())
            return scanner
    monkeypatch.setattr(search, 'open_lance_dataset', lambda *a, **kw: Watched(native_open(*a, **kw)))
    for use_index in (True, False):
        rows, _ = collect(plan(uri, [dict(vector=vectors[73].tolist())], version=version,
            options={'use_index': use_index, 'nprobes': 2, 'refine_factor': 2}))
        assert rows[0]['hits'][0]['id'] == 73
    assert 'ANN' in plans[0]
    assert 'ANN' not in plans[1]


def test_failure_drains_other_search_before_closing_handle(table, monkeypatch):
    entered = threading.Barrier(2, timeout=5)
    completed = threading.Event()
    closed = []
    real_call, real_close = search.VectorSearch.__call__, search.VectorSearch.aclose
    def run(self, row):
        self._open()
        entered.wait()
        if row['bad']:
            raise ValueError('broken query')
        time.sleep(.05)
        result = real_call(self, row)
        completed.set()
        return result
    async def close(self):
        assert completed.is_set()
        await real_close(self)
        closed.append(True)
    monkeypatch.setattr(search.VectorSearch, '__call__', run)
    monkeypatch.setattr(search.VectorSearch, 'aclose', close)
    with pytest.raises(ValueError, match='broken query'):
        plan(table[0], [dict(vector=[1., 0.], bad=True), dict(vector=[0., 1.], bad=False)]).run_stream()
    assert closed == [True]


def test_concurrency_and_slow_consumer_backpressure(table, monkeypatch):
    active = peak = finished = consumed = max_ahead = 0
    lock = threading.Lock()
    real_call = search.VectorSearch.__call__
    def run(self, row):
        nonlocal active, peak, finished, max_ahead
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(.005)
            return real_call(self, row)
        finally:
            with lock:
                active -= 1
                finished += 1
                max_ahead = max(max_ahead, finished - consumed)
    async def consume(row):
        nonlocal consumed
        await asyncio.sleep(.01)
        consumed += 1
        return row
    monkeypatch.setattr(search.VectorSearch, '__call__', run)
    stream = plan(table[0], [dict(vector=[1., 0.])] * 20)
    stats = stream.map_async(consume, concurrency=1, queue_depth=1).run_stream()
    assert 1 < peak <= 2 and active == 0
    assert finished == consumed == stats.emitted == 20
    assert max_ahead <= 4  # Two search workers, downstream queue and consumer.


@pytest.fixture
def hnsw_table(tmp_path):
    """Real SQ graph, with query vectors held out from the stored corpus."""
    import numpy as np
    vectors = np.random.default_rng(18).normal(size=(260, 16)).astype('float32')
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    uri = str(tmp_path / 'hnsw.lance')
    schema = pa.schema([('id', pa.int64()), ('group', pa.string()),
                        ('embedding', pa.list_(pa.float32(), 16))])
    lance.write_dataset(pa.table({'id':list(range(256)),
        'group':['a' if i % 2 else 'b' for i in range(256)],
        'embedding':pa.FixedSizeListArray.from_arrays(pa.array(vectors[:256].ravel()),16)},
        schema=schema), uri)
    ds = lance.dataset(uri)
    ds.create_index('embedding', 'IVF_HNSW_SQ', name='search_hnsw',
        metric='cosine', num_partitions=2, m=16, ef_construction=100)
    assert ds.stats.index_stats('search_hnsw')['index_type'] == 'IVF_HNSW_SQ'
    return uri, ds.version, schema, vectors[256:]


def watch_searches(monkeypatch):
    """Observe real native calls and plans without substituting their results."""
    opened, scanned = [], []
    native_open = search.open_lance_dataset
    class Watched:
        def __init__(self, ds): self.ds = ds
        def __getattr__(self, name): return getattr(self.ds, name)
        def scanner(self, **kwargs):
            scanner = self.ds.scanner(**kwargs)
            scanned.append((kwargs, scanner.explain_plan()))
            return scanner
    def open_table(*args, **kwargs):
        opened.append(args)
        return Watched(native_open(*args, **kwargs))
    monkeypatch.setattr(search, 'open_lance_dataset', open_table)
    return opened, scanned


@pytest.mark.parametrize('ef', [None, 320])
def test_hnsw_refinement_stream_chain_matches_exact(hnsw_table, monkeypatch, ef):
    uri, version, _, queries = hnsw_table
    rows = [dict(qid=i, vector=q.tolist()) for i,q in enumerate(queries)]
    exact = {r['qid']:r['hits'] for r in collect(plan(uri, rows, metric='cosine',
        version=version, top_k=20, options={'use_index':False}))[0]}
    opened, scanned = watch_searches(monkeypatch)
    async def upstream(row):
        await asyncio.sleep(.001)
        return row
    # Large refinement scope makes this a deterministic contract check, not
    # a performance recommendation: the expanded k covers the small corpus.
    options = dict(nprobes=2, refine_factor=16, ef=ef)
    stream = (data.from_items(rows).map_async(upstream, concurrency=2, queue_depth=1)
        .search_vectors(query='vector', output='hits', uri=uri, version=version,
            vector_column='embedding', columns=['id'], top_k=20, metric='cosine',
            concurrency=2, queue_depth=1, options=options))
    actual = stream.materialize().take_all()
    assert len(actual) == len(rows) and len(opened) == 1
    for row in actual:
        assert row['vector'] == rows[row['qid']]['vector']
        assert [h['id'] for h in row['hits']] == [h['id'] for h in exact[row['qid']]]
        assert [h['_distance'] for h in row['hits']] == pytest.approx(
            [h['_distance'] for h in exact[row['qid']]], abs=1e-6)
    assert all('ANN' in explain and 'search_hnsw' in explain for _,explain in scanned)
    assert all(kwargs['nearest'].get('ef') == ef for kwargs,_ in scanned)
    assert all(kwargs['nearest']['refine_factor'] == 16 for kwargs,_ in scanned)
    assert all(kwargs['columns'] == ['id', '_distance'] for kwargs,_ in scanned)
    assert stream._stages[-1]._dataset is None


def test_hnsw_prefilter_empty_and_fewer_than_top_k(hnsw_table):
    uri, version, _, queries = hnsw_table
    rows = [dict(vector=queries[0].tolist(), predicate='id < 12'),
            dict(vector=queries[1].tolist(), predicate='id < 0')]
    actual, _ = collect(plan(uri, rows, version=version, metric='cosine', top_k=20,
        filter="group = 'a'", filter_column='predicate',
        options={'nprobes':2, 'ef':320, 'refine_factor':16}))
    result = {r['predicate']:r['hits'] for r in actual}
    assert {h['id'] for h in result['id < 12']} == {1,3,5,7,9,11}
    assert result['id < 0'] == []


def test_hnsw_snapshot_and_unindexed_append(hnsw_table, monkeypatch):
    uri, indexed_version, schema, queries = hnsw_table
    vector = queries[0].tolist()
    new = lance.write_dataset(pa.Table.from_pylist([
        dict(id=999, group='a', embedding=vector)], schema=schema), uri, mode='append')
    opened, scanned = watch_searches(monkeypatch)
    kwargs = dict(metric='cosine', top_k=10, options={'nprobes':2, 'ef':64, 'refine_factor':2})
    old, _ = collect(plan(uri, [dict(vector=vector)], version=indexed_version, **kwargs))
    current, _ = collect(plan(uri, [dict(vector=vector)], version=new.version, **kwargs))
    assert 999 not in {hit['id'] for hit in old[0]['hits']}
    assert current[0]['hits'][0]['id'] == 999
    assert current[0]['hits'][0]['_distance'] == pytest.approx(0, abs=1e-6)
    assert all('ANN' in explain for _,explain in scanned)
    assert len(opened) == 2


def test_hnsw_exact_override_ignores_ann_controls(hnsw_table, monkeypatch):
    uri, version, _, queries = hnsw_table
    _, scanned = watch_searches(monkeypatch)
    result, _ = collect(plan(uri, [dict(vector=queries[0].tolist())], version=version,
        metric='cosine', top_k=20, options={'use_index':False, 'nprobes':1,
            'ef':1, 'refine_factor':10000, 'max_search_candidates':20}))
    assert len(result[0]['hits']) == 20
    assert 'ANN' not in scanned[0][1]
    assert not {'nprobes','ef','refine_factor'} & scanned[0][0]['nearest'].keys()


@pytest.mark.parametrize('indexed', [False, True])
def test_top_k_order_restored_when_native_batches_arrive_out_of_order(hnsw_table, monkeypatch, indexed):
    """Native flat search on the published pool delivered its 36-row tail first.

    Reorder real result batches deterministically to cover both reader paths;
    compare the complete top-100 contents, plus the prefix consumed downstream.
    """
    uri, version, _, queries = hnsw_table
    options = dict(use_index=indexed, nprobes=2, ef=500, refine_factor=5, batch_size=32)
    args = dict(version=version, metric='cosine', top_k=100, options=options)
    inputs = [dict(vector=queries[0].tolist())]
    expected = collect(plan(uri, inputs, **args))[0][0]['hits']
    native_open = search.open_lance_dataset
    batch_counts = []

    class ReorderedScanner:
        def __init__(self, scanner): self.scanner = scanner
        def to_batches(self):
            # The small fixture may be returned as one batch; split its real
            # top-100 data into chunks before simulating reader completion order.
            batches = [batch.slice(offset, 32) for batch in self.scanner.to_batches()
                       for offset in range(0, batch.num_rows, 32)]
            batch_counts.append(len(batches))
            yield from reversed(batches)

    class ReorderedTable:
        def __init__(self, ds): self.ds = ds
        def __getattr__(self, name): return getattr(self.ds, name)
        def scanner(self, **kwargs): return ReorderedScanner(self.ds.scanner(**kwargs))

    monkeypatch.setattr(search, 'open_lance_dataset', lambda *a, **kw: ReorderedTable(native_open(*a, **kw)))
    actual = collect(plan(uri, inputs, **args))[0][0]['hits']
    assert batch_counts and batch_counts[0] > 1
    assert len(actual) == 100
    assert [hit['_distance'] for hit in actual] == sorted(hit['_distance'] for hit in actual)
    assert [hit['id'] for hit in actual] == [hit['id'] for hit in expected]
    assert [hit['id'] for hit in actual[:20]] == [hit['id'] for hit in expected[:20]]


@pytest.mark.parametrize('options', [
    {'ef':1001, 'max_search_candidates':1000},
    {'refine_factor':100, 'ef':2000, 'max_search_candidates':1000},
    {'refine_factor':5, 'max_search_candidates':149},  # Native default ef = 150.
])
def test_candidate_budget_precedes_table_io(options, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('over-budget declaration opened a table')
    monkeypatch.setattr(search, 'open_lance_dataset', forbidden)
    with pytest.raises(MemoryError, match='max_search_candidates'):
        plan('/missing.lance', [], top_k=20, options=options)
