"""A source is read by fewer partitions rather than parking stages that could stream together.

When the plan cannot keep every stage resident, the planner tries narrower source
widths before parking an edge. A parked window moves a bounded number of rows per
checkpoint and relaunches its stages each time; a narrower source keeps streaming.
"""
from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution


def _files(tmp_path, count, lines=5):
    paths = []
    for f in range(count):
        path = tmp_path / ('part-%02d.txt' % f)
        path.write_text(''.join('line %d of %d\n' % (i, f) for i in range(lines)))
        paths.append(str(path))
    return paths


def _launches(monkeypatch):
    plans = []
    prepare = DataflowExecution._prepare_launch
    def capture(self, *args, **kwargs):
        plans.append(self._physical)
        return prepare(self, *args, **kwargs)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', capture)
    return plans


def _source_partitions(physical):
    sources = {snapshot.source_id for snapshot in physical.sources}
    (stage,) = [s for s in physical.stages if sources & set(s.occurrences)]
    return len(stage.partitions)


def _key(row):
    return {'k': int(row['text'].split()[1]) % 2, 'text': row['text']}


def _count(state, row):
    return {'k': row['k'], 'n': (state or {}).get('n', 0) + 1}


def test_many_files_are_read_by_fewer_partitions_instead_of_parking(tmp_path, monkeypatch):
    launches = _launches(monkeypatch)
    with data.local_execution(workers=4, worker_mode='thread'):
        rows = data.read_text(_files(tmp_path, 8)).take_all()
    assert sorted(r['text'] for r in rows) == sorted('line %d of %d' % (i, f) for f in range(8) for i in range(5))
    (physical,) = {p.plan_id: p for p in launches}.values()
    # Four readers beside the result stage do not fit the default budget.
    assert _source_partitions(physical) < 4
    assert [e.mode for e in physical.exchanges] == ['memory_incremental']
    # Every worker starts once; a parked window relaunched them per checkpoint.
    assert len(launches) == sum(len(s.partitions) for s in physical.stages)


def test_a_source_too_wide_for_the_budget_is_narrowed_instead_of_refused(tmp_path, monkeypatch):
    launches = _launches(monkeypatch)
    with data.local_execution(workers=8, worker_mode='thread'):
        rows = data.read_text(_files(tmp_path, 8)).map(_key).reduce_by_key('k', _count).take_all()
    assert sorted((r['k'], r['n']) for r in rows) == [(0, 24), (1, 16)]
    physical = launches[0]
    assert 1 < _source_partitions(physical) < 8
    assert physical.exchanges[0].mode == 'memory_incremental'


def test_a_narrower_source_that_parks_no_fewer_edges_keeps_its_width(tmp_path, monkeypatch):
    # The reducer and its consumer must be parked at any source width; the two
    # readers and the map fit together, so reading with one would only be slower.
    launches = _launches(monkeypatch)
    with data.local_execution(workers=4, worker_mode='thread'):
        rows = data.read_text(_files(tmp_path, 2)).map(_key).reduce_by_key('k', _count).take_all()
    assert sorted((r['k'], r['n']) for r in rows) == [(0, 6), (1, 4)]
    physical = launches[0]
    assert _source_partitions(physical) == 2
    assert [e.mode for e in physical.exchanges] == ['memory_incremental', 'finite_sealed', 'finite_sealed']
