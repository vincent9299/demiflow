import pytest
from demiflow import data


def test_process_cache_reuses_exact_inputs_and_keeps_failed_input_retryable(tmp_path):
    marker=tmp_path/'allowed'
    output=tmp_path/'calls'
    output.mkdir()
    def compute(row):
        with (output/str(row['id'])).open('a') as stream: stream.write('call\n')
        if row['id']==2 and not marker.exists(): raise ValueError('not ready')
        return {**row,'answer':row['value']*2}
    def execute(rows,version='v1'):
        with data.local_execution(workers=1,partitions=2):
            return data.from_items(rows).map_cached(compute,cache_dir=tmp_path/'cache',version=version,synchronous=True).take(10)
    first=[{'id':1,'value':10}]
    assert execute(first)==[{'id':1,'value':10,'answer':20}]
    with pytest.raises(ValueError,match='not ready'): execute([*first,{'id':2,'value':20}])
    marker.touch()
    assert len(execute([*first,{'id':2,'value':20}]))==2
    assert (output/'1').read_text().count('call')==1
    execute([{'id':1,'value':11}])
    execute(first,'v2')
    assert (output/'1').read_text().count('call')==3
    assert list((tmp_path/'cache').rglob('*.error.json'))


def test_sync_cache_rejects_async_callable(tmp_path):
    async def compute(row): return row
    with pytest.raises(TypeError,match='async callables'):
        data.from_items([]).map_cached(compute,cache_dir=tmp_path,version='v1',synchronous=True)


@pytest.mark.parametrize('synchronous', [False, True])
def test_cache_predicate_keeps_unsubmitted_rows_retryable(tmp_path, synchronous):
    calls = []
    enabled = []
    def compute(row):
        calls.append(row['id'])
        return {**row, 'status': 'done' if row['id'] == 1 or enabled else 'budget_exhausted'}
    async def async_compute(row): return compute(row)
    def plan():
        result = data.from_items([{'id':1}, {'id':2}]).map_cached(compute if synchronous else async_compute,
            cache_dir=tmp_path/'cache', version='v1', synchronous=synchronous,
            cache_when=lambda row: row['status'] == 'done')
        return (result if synchronous else result.materialize()).take(10)
    assert {r['status'] for r in plan()} == {'done', 'budget_exhausted'}
    enabled.append(True)
    assert {r['status'] for r in plan()} == {'done'}
    plan()
    assert calls.count(1) == 1 and calls.count(2) == 2


def test_cache_predicate_rejects_async_result(tmp_path):
    async def predicate(row): return True
    with pytest.raises(TypeError, match='boolean synchronously'):
        data.from_items([{'id':1}]).map_cached(lambda row: row,
            cache_dir=tmp_path, version='v1', synchronous=True, cache_when=predicate).take(1)
