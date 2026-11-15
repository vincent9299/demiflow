"""Public API behavior census; real Dataset, provider and original run journal.

Every entry is classified. A missing positive, missing error-boundary example,
or environmental prerequisite is an explicit gap, never a counted pass.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import asyncio
import hashlib
from functools import partial
import json
import math
from pathlib import Path
import re
import sqlite3
import traceback

WORKTREE = Path(__file__).resolve().parents[1]
ROWS = [{'id': 2, 'k': 'b', 'v': 5}, {'id': 1, 'k': 'a', 'v': 3}, {'id': 1, 'k': 'a', 'v': 3}]


def _same(expected, actual, *, ordered=True):
    from runtime_v3_support import _assert_deep_equal, _assert_multiset_equal
    (_assert_deep_equal if ordered else _assert_multiset_equal)(expected, actual, path='api-output')
    return {'rows': len(actual)} if isinstance(actual, list) else {'value': actual}


def _ds():
    from demiflow import data
    return data.from_items(ROWS)


def _inc(row):
    return {**row, 'v': row['v'] + 1}


def _filter(row):
    return row['k'] == 'a'


def _flat(row):
    return [row, row]


async def _ainc(row):
    return _inc(row)


async def _abatch(rows):
    return [_inc(row) for row in rows]


def _column(batch):
    return batch['v'] * 2


def _arrow_batch(batch):
    import pyarrow.compute as pc
    return batch.set_column(batch.schema.get_field_index('v'), 'v', pc.add(batch['v'], 1))


def _reduce(acc, row, *, factor=1):
    return {'k': row['k'], 'v': (acc or {}).get('v', 0) + factor * row['v']}


def _iterate():
    yield from ROWS


def _sql_connect(path):
    return sqlite3.connect(path)


# This is an actual user Datasource, not an executor/DTO substitute.
from demiflow.data.datasource import Datasource, ReadTask, BlockMetadata
class OwnedSource(Datasource):
    def snapshot_identity(self):
        return {'schema': 'api-owned-fixture.v1', 'rows': ROWS}
    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        return [ReadTask(_owned_blocks, BlockMetadata(num_rows=3))]


def _owned_blocks():
    import pyarrow as pa
    yield pa.Table.from_pylist(ROWS)


@dataclass(frozen=True)
class Case:
    name: str
    apis: tuple[str, ...]
    group: str
    run: object
    error: tuple = ()
    match: str | None = None
    requires_execution: bool = True
    kind: str = 'positive'
    variants: tuple[str, ...] = ()
    self_managed: bool = False


def _fixtures(root):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import lance
    from PIL import Image
    root.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(ROWS)
    (root/'rows.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in ROWS))
    (root/'rows.csv').write_text('id,k,v\n2,b,5\n1,a,3\n1,a,3\n')
    (root/'lines.txt').write_text('first\n\nlast\n')
    (root/'binary.bin').write_bytes(b'\x00binary\xff')
    (root/'records.jsonl').write_text('{"id":1}\ninvalid\n{"id":2}\n')
    pq.write_table(table, root/'rows.parquet')
    lance.write_dataset(table, str(root/'rows.lance'), mode='create')
    vectors = pa.table({'id': [1, 2], 'vector': pa.array([[1., 0.], [0., 1.]], type=pa.list_(pa.float32(), 2))})
    lance.write_dataset(vectors, str(root/'vectors.lance'), mode='create')
    Image.new('RGB', (2, 1), (10, 20, 30)).save(root/'image.png')
    with sqlite3.connect(root/'input.sqlite') as db:
        db.execute('CREATE TABLE rows(id INTEGER,k TEXT,v INTEGER)')
        db.executemany('INSERT INTO rows VALUES (:id,:k,:v)', ROWS)
    return root


def _cases(source):
    from demiflow import data
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.recovery_contract import RecoveryContractError
    import pyarrow as pa
    import numpy as np
    import pandas as pd
    cases = []
    def pos(name, apis, group, run, *variants, execution=True):
        cases.append(Case(name, tuple(apis.split(',')), group, run, requires_execution=execution, variants=variants))
    def neg(name, api, group, run, error=(ValueError, TypeError), match=None, execution=False):
        cases.append(Case(name, (api,), group, run, error, match, execution, 'negative'))
    def take(ds):
        return _same(ROWS, ds.take_all())
    pos('literal-items', 'from_items,Dataset.take_all', 'sources', lambda r: take(data.from_items(ROWS)), 'list dicts; order and duplicate multiplicity')
    pos('literal-python-types', 'from_items,Dataset.map,Dataset.take_all', 'sources', _literal_types, 'tuple/list/bytes/-0.0 preserved through actual worker')
    pos('iterator-factory', 'from_iter,Dataset.iter_rows', 'sources', lambda r: _same(ROWS, list(data.from_iter(_iterate).iter_rows())), 'zero-arg reusable factory')
    pos('range', 'range,Dataset.count', 'sources', lambda r: _same(4, data.range(4).count()), 'lazy row count')
    pos('arrow', 'from_arrow', 'sources', lambda r: take(data.from_arrow(pa.Table.from_pylist(ROWS))), 'Arrow table')
    pos('pandas', 'from_pandas', 'sources', lambda r: take(data.from_pandas(pd.DataFrame(ROWS))), 'DataFrame')
    pos('numpy', 'from_numpy', 'sources', lambda r: _numpy_read(), 'int64 ndarray')
    for name, arg in [('from_items', iter(ROWS)), ('from_iter', ROWS), ('from_arrow', None), ('from_numpy', 1), ('from_pandas', 1)]:
        neg('bad-'+name, name, 'sources', lambda r,n=name,a=arg: getattr(data,n)(a), (TypeError,DataflowError), 'requires')
    neg('negative-range', 'range', 'sources', lambda r: data.range(-1), match='non-negative')
    for name, suffix in [('read_json','jsonl'), ('read_csv','csv'), ('read_parquet','parquet')]:
        pos(name, name, 'readers', lambda r,n=name,s=suffix: _same([{k:str(v) for k,v in row.items()} for row in ROWS] if n=='read_csv' else ROWS,getattr(data,n)(str(source/('rows.'+s))).take_all()), 'local file')
        neg('empty-'+name, name, 'readers', lambda r,n=name:getattr(data,n)([]), match='at least one path')
    pos('read-lance-version', 'read_lance', 'readers', lambda r: take(data.read_lance(str(source/'rows.lance'),version=1,batch_size=1)), 'pinned version; bounded batches')
    neg('lance-bad-version','read_lance','readers',lambda r:data.read_lance(str(source/'rows.lance'),version=0), match='version')
    pos('sql-sqlite','read_sql','readers', lambda r: take(data.read_sql('SELECT id,k,v FROM rows ORDER BY rowid', partial(_sql_connect,str(source/'input.sqlite')))), 'read-only SELECT over owned SQLite input')
    neg('sql-invalid-query','read_sql','readers',lambda r:data.read_sql('SELECT invalid_column FROM rows',partial(_sql_connect,str(source/'input.sqlite'))).take_all(),(Exception,), 'invalid_column', execution=True)
    pos('datasource','read_datasource','readers',lambda r:take(data.read_datasource(OwnedSource())), 'real user Datasource/ReadTask')
    neg('datasource-unsupported-option','read_datasource','readers',lambda r:data.read_datasource(OwnedSource(),magic=1),match='unsupported')
    pos('text-empty-lines','read_text','readers',lambda r:_same([{'text':'first'},{'text':''},{'text':'last'}],data.read_text(str(source/'lines.txt'),drop_empty_lines=False).take_all()), 'drop_empty_lines=False')
    pos('binary','read_binary_files','readers',lambda r:_same([{'bytes':b'\x00binary\xff'}],data.read_binary_files(str(source/'binary.bin')).take_all()), 'binary bytes')
    pos('image','read_images','readers',lambda r:_image_read(source), 'RGB 2x1; ndarray pixel values')
    pos('records-provenance','read_records','readers',lambda r:_record_read(source), 'bad JSON retained with line provenance')
    for name in ('read_text','read_binary_files','read_images'):
        neg('empty-'+name,name,'readers',lambda r,n=name:getattr(data,n)([]),match='at least one path')
    neg('empty-read-records','read_records','readers',lambda r:data.read_records([]),match='requires paths')
    pos('map-bindings','Dataset.map','transforms',lambda r:_same([{'id':x['id'],'k':x['k'],'v':x['v'],'out':x['v']*2} for x in ROWS],_ds().map(lambda value,factor:value*factor,inputs={'value':'v'},output='out',fn_kwargs={'factor':2}).take_all()), 'inputs/output/fn_kwargs')
    pos('flat-map','Dataset.flat_map','transforms',lambda r:_same([x for row in ROWS for x in (row,row)],_ds().flat_map(_flat).take_all()), 'duplicate expansion')
    pos('map-async','Dataset.map_async','transforms',lambda r:_same([_inc(x) for x in ROWS],_ds().map_async(_ainc,concurrency=1).take_all()), 'async callable')
    pos('batch-map','Dataset.batch_map','transforms',lambda r:_same([_inc(x) for x in ROWS],_ds().batch_map(_abatch,max_batch=2,concurrency=1).take_all()), 'bounded async batches and final partial batch')
    pos('map-batches-arrow','Dataset.map_batches','transforms',lambda r:_same([_inc(x) for x in ROWS],_ds().map_batches(_arrow_batch,batch_size=2,batch_format='pyarrow').take_all()), 'Arrow format; final partial batch')
    pos('projection-filter','Dataset.filter,Dataset.select_columns,Dataset.drop_columns,Dataset.rename_columns,Dataset.limit','transforms',lambda r:_same([{'value':3}],_ds().filter(_filter).select_columns(['id','v']).drop_columns('id').rename_columns({'v':'value'}).limit(1).take_all()), 'predicate, projection, rename mapping, early limit')
    pos('add-column','Dataset.add_column','transforms',lambda r:_same([{**x,'twice':x['v']*2} for x in ROWS],_ds().add_column('twice',_column).take_all()), 'pandas batch column')
    pos('random-sample-full','Dataset.random_sample','transforms',lambda r:take(_ds().random_sample(1.,seed=5)), 'fraction 1 preserves all duplicates')
    for name in ('map','flat_map','filter'):
        neg('not-callable-'+name,'Dataset.'+name,'transforms',lambda r,n=name:getattr(_ds(),n)(None),match='callable')
    neg('async-bad-concurrency','Dataset.map_async','transforms',lambda r:_ds().map_async(_ainc,concurrency=0),match='concurrency')
    neg('batch-map-zero','Dataset.batch_map','transforms',lambda r:_ds().batch_map(_abatch,max_batch=0),match='max_batch')
    neg('map-batches-format','Dataset.map_batches','transforms',lambda r:_ds().map_batches(_arrow_batch,batch_format='invalid'),match='batch_format')
    for name, arg in [('select_columns',[]),('drop_columns',['id','id']),('rename_columns',{'id':'v','k':'v'}),('limit',-1)]:
        neg('invalid-'+name,'Dataset.'+name,'transforms',lambda r,n=name,a=arg:getattr(_ds(),n)(a))
    neg('add-empty-column','Dataset.add_column','transforms',lambda r:_ds().add_column('',_column),match='non-empty')
    neg('sample-invalid-fraction','Dataset.random_sample','transforms',lambda r:_ds().random_sample(2),match='fraction')
    pos('sort-multiple','Dataset.sort','relations',lambda r:_same(sorted(ROWS,key=lambda x:(x['k'],-x['v'])),_ds().sort(['k','v'],descending=[False,True]).take_all()), 'multiple keys and per-key descending')
    pos('union','Dataset.union','relations',lambda r:_same(ROWS+ROWS,_ds().union(_ds()).take_all(),ordered=False), 'bag union preserves duplicates')
    pos('join-duplicate','Dataset.join','relations',lambda r:_join_case(), 'duplicate keys multiply; suffix; no order guarantee')
    pos('reduce-ordered','Dataset.reduce_by_key','relations',lambda r:_same([{'k':'a','v':12},{'k':'b','v':10}],_ds().reduce_by_key('k',_reduce,fn_kwargs={'factor':2}).sort('k').take_all()), 'fn_kwargs and initial None; non-associative row fold')
    pos('group-complete','Dataset.group_batches','relations',lambda r:_groups(), 'group_index/group_last; bounded max_rows')
    pos('exclude-pinned','Dataset.exclude_keys','relations',lambda r:_same([ROWS[0]],data.read_lance(str(source/'rows.lance'),version=1).exclude_keys(data.from_items([{'k':'a'}]),on='k').take_all()), 'untransformed fixed Lance source; anti keys')
    pos('repartition','Dataset.repartition','relations',lambda r:_same(ROWS,_ds().repartition(num_blocks=2,shuffle=True,keys=['k']).take_all(),ordered=False), 'key shuffle; bag semantics')
    pos('shuffle','Dataset.random_shuffle','relations',lambda r:_same(ROWS,_ds().random_shuffle(seed=11,num_blocks=2).take_all(),ordered=False), 'seed and output partitions')
    pos('block-randomize','Dataset.randomize_block_order','relations',lambda r:_same(ROWS,_ds().randomize_block_order(seed=11).take_all(),ordered=False), 'block order does not lose rows')
    neg('sort-direction-arity','Dataset.sort','relations',lambda r:_ds().sort(['id','v'],descending=[True]),match='one bool per key')
    neg('union-not-dataset','Dataset.union','relations',lambda r:_ds().union(1))
    neg('join-invalid-how','Dataset.join','relations',lambda r:_ds().join(_ds(),on='id',how='invalid'))
    neg('reduce-not-callable','Dataset.reduce_by_key','relations',lambda r:_ds().reduce_by_key('k',None))
    neg('group-zero-rows','Dataset.group_batches','relations',lambda r:_ds().group_batches('k',max_rows=0))
    neg('exclude-wrong-source','Dataset.exclude_keys','relations',lambda r:_ds().exclude_keys(_ds(),on='k'),match='untransformed read_lance')
    neg('repartition-two-controls','Dataset.repartition','relations',lambda r:_ds().repartition(num_blocks=2,target_num_rows_per_block=1),match='exactly one')
    neg('shuffle-invalid-seed','Dataset.random_shuffle','relations',lambda r:_ds().random_shuffle(seed=True),match='seed')
    neg('block-order-invalid-seed','Dataset.randomize_block_order','relations',lambda r:_ds().randomize_block_order(seed=True),match='seed')
    pos('take-bound','Dataset.take','terminals',lambda r:_same(ROWS[:2],_ds().take(2)), 'bounded take')
    pos('take-batch-arrow','Dataset.take_batch','terminals',lambda r:_same(ROWS[:2],_ds().take_batch(2,batch_format='pyarrow').to_pylist()), 'Arrow batch result')
    pos('iter-batches','Dataset.iter_batches','terminals',lambda r:_iter_batches(), 'batch_size 2; drop_last true/false')
    pos('require-columns','Dataset.require_columns','terminals',lambda r:_same(None,_ds().require_columns(['id','k','v'])), 'exact schema prerequisite')
    pos('count-duplicates','Dataset.count','terminals',lambda r:_same(3,_ds().count()), 'count retains duplicate multiplicity')
    pos('summary','Dataset.collect_summary','terminals',lambda r:_same(ROWS,_ds().collect_summary(max_rows=3,max_bytes=4096)), 'explicit row and byte budgets')
    pos('schema-columns-size','Dataset.schema,Dataset.columns,Dataset.size_bytes','terminals',lambda r:_schema(), 'schema names/types; actual encoded bytes')
    pos('materialized-lifecycle','Dataset.materialize,MaterializedDataset.num_blocks,MaterializedDataset.release','terminals',lambda r:_materialized(), 'current-run durable materialization; pin release and use-after-release rejection')
    pos('diagnostics','Dataset.stats,Dataset.execution_metadata','terminals',lambda r:_diagnostics(), 'real completed action diagnostic observation')
    from demiflow.data.aggregate import Sum, Count
    pos('aggregate','Dataset.aggregate','terminals',lambda r:_same({'sum(v)':11,'count()':3},_ds().aggregate(Sum('v'),Count())), 'multiple named aggregates')
    for name,want in [('sum',11),('min',3),('max',5),('mean',11/3),('std',math.sqrt(4/3))]:
        pos('aggregate-'+name,'Dataset.'+name,'terminals',lambda r,n=name,w=want:_numeric(w,getattr(_ds(),n)('v')), 'scalar column wrapper executes aggregate action')
        neg('bad-column-'+name,'Dataset.'+name,'terminals',lambda r,n=name:getattr(_ds(),n)('missing'),(Exception,),'missing', execution=True)
    neg('negative-take','Dataset.take','terminals',lambda r:_ds().take(-1),match='non-negative')
    neg('take-all-overflow','Dataset.take_all','terminals',lambda r:_ds().take_all(limit=2),match='limit',execution=True)
    neg('batch-bad-format','Dataset.take_batch','terminals',lambda r:_ds().take_batch(batch_format='invalid'),match='batch_format')
    neg('iter-bad-batch','Dataset.iter_batches','terminals',lambda r:list(_ds().iter_batches(batch_size=0)),match='batch_size')
    neg('require-missing-column','Dataset.require_columns','terminals',lambda r:_ds().require_columns(['missing']),match='columns',execution=True)
    neg('summary-overflow','Dataset.collect_summary','terminals',lambda r:_ds().collect_summary(max_rows=2),match='budget',execution=True)
    neg('aggregate-empty','Dataset.aggregate','terminals',lambda r:_ds().aggregate(),match='at least one')
    for name in ('checkpoint','checkpoint_async','checkpoint_lance','checkpoint_lance_async'):
        neg('retired-'+name,'Dataset.'+name,'retired',lambda r,n=name:_retired(n,r),(Exception,),'retired|not supported|unsupported|execution-owned|not an executable',execution=False)
    for name in ('json','csv','parquet','lance'):
        pos('write-'+name,'Dataset.write_'+name,'writers',lambda r,n=name:_write_read(n,r), 'actual destination readback and native original commit')
        neg('invalid-writer-'+name,'Dataset.write_'+name,'writers',lambda r,n=name:getattr(_ds(),'write_'+n)(str(r/'bad'),backend_options={'unknown_option':1}),(Exception,),'unknown|unsupported|backend_options requires only a ray mapping',execution=False)
    cases.extend(_extra_cases(source))
    cases.extend(_integration_cases(source))
    return cases


def _literal_types(root):
    from demiflow import data
    rows=[{'value': {'tuple':(b'x',-0.0),'list':[b'y',0.0]}}]
    return _same(rows,data.from_items(rows).map(lambda x:x).take_all())


def _numpy_read():
    from demiflow import data
    import numpy as np
    output=data.from_numpy(np.array([2,1,1],dtype=np.int64)).take_all()
    assert len(output)==3 and all(isinstance(row['data'],np.int64) for row in output)
    assert [int(row['data']) for row in output]==[2,1,1]
    return {'dtype':'int64','rows':3}


def _image_read(source):
    from demiflow import data
    import numpy as np
    row=data.read_images(str(source/'image.png'),size=(1,1),mode='L').take_all()[0]
    assert isinstance(row['image'],np.ndarray) and row['image'].dtype==np.uint8
    assert row['image'].shape==(1,1) and row['image'].tolist()==[[18]]
    assert 'path' not in row
    return {'shape':list(row['image'].shape),'dtype':str(row['image'].dtype)}


def _record_read(source):
    from demiflow import data
    rows=data.read_records(str(source/'records.jsonl')).take_all()
    assert len(rows)==3 and rows[0]['value']=={'id':1} and rows[2]['value']=={'id':2}
    assert rows[1]['error'] and rows[1]['value'] is None
    return {'records':3,'bad_record_preserved':rows[1]}


def _join_case():
    from demiflow import data
    right=data.from_items([{'id':1,'tag':'x'},{'id':1,'tag':'y'}])
    expected=[{**row,'tag':tag} for row in ROWS if row['id']==1 for tag in ('x','y')]
    return _same(expected,_ds().join(right,on='id').take_all(),ordered=False)


def _groups():
    rows=_ds().group_batches('k',max_rows=1).take_all()
    _same(ROWS,[item for group in rows for item in group['items']],ordered=False)
    assert len(rows)==3
    for key in ('a','b'):
        group=sorted((x for x in rows if x['k']==key),key=lambda x:x['group_index'])
        assert [x['group_index'] for x in group]==list(range(len(group)))
        assert [x['group_last'] for x in group]==[False]*(len(group)-1)+[True]
    return {'groups':rows}


def _iter_batches():
    batches=list(_ds().iter_batches(batch_size=2,batch_format='pyarrow'))
    assert [b.num_rows for b in batches]==[2,1]
    _same(ROWS,[row for batch in batches for row in batch.to_pylist()])
    dropped=list(_ds().iter_batches(batch_size=2,batch_format='pyarrow',drop_last=True))
    return _same(ROWS[:2],[row for batch in dropped for row in batch.to_pylist()])


def _schema():
    schema=_ds().schema()
    assert schema.names==['id','k','v']
    _same(['id','k','v'],_ds().columns())
    size=_ds().size_bytes()
    assert type(size) is int and size>0
    return {'schema':str(schema),'size_bytes':size}


def _materialized():
    materialized=_ds().map(_inc).materialize()
    blocks=materialized.num_blocks()
    assert blocks>0
    _same([_inc(x) for x in ROWS],materialized.take_all())
    materialized.release(); materialized.release()
    try: materialized.take_all()
    except Exception as exc:
        assert 'released' in str(exc)
        return {'blocks':blocks,'after_release':{'type':type(exc).__name__,'message':str(exc)}}
    raise AssertionError('Released materialized handle remained readable')


def _diagnostics():
    ds=_ds().map(_inc)
    _same([_inc(x) for x in ROWS],ds.take_all())
    stats=ds.stats(); metadata=ds.execution_metadata()
    assert isinstance(stats,str) and stats
    assert metadata is not None
    return {'stats':stats[:2000],'metadata_type':type(metadata).__name__}


def _numeric(expected,actual):
    assert math.isclose(expected,actual,rel_tol=1e-12), (expected,actual)
    return {'value':actual}


def _retired(name,root):
    kwargs={'schema':None,'fingerprint':'owned'} if 'lance' in name else {'version':'owned'}
    value=getattr(_ds(),name)(str(root/'retired'),**kwargs)
    if name.endswith('_async'): return asyncio.run(value)
    return value


def _write_read(kind,root):
    from demiflow import data
    target=root/('output.'+kind)
    kwargs={'mode':'create','return_receipt':True} if kind=='lance' else {}
    receipt=getattr(_ds(),'write_'+kind)(str(target),**kwargs)
    if kind=='lance': assert receipt.status=='committed' and receipt.written_rows==3
    else: assert receipt is None
    return _same([{k:str(v) for k,v in row.items()} for row in ROWS] if kind=='csv' else ROWS,getattr(data,'read_'+kind)(str(target)).take_all(),ordered=False)


NA_NEGATIVE={
    'Dataset.count':'No parameters; positive case covers duplicate and count semantics.',
    'Dataset.iter_rows':'No parameters; iterator is fully consumed and values/types/order checked.',
    'Dataset.materialize':'No parameters; positive also checks idempotent release and use-after-release.',
    'MaterializedDataset.num_blocks':'No parameters; positive checks nonzero pinned block count.',
    'MaterializedDataset.release':'No parameters; idempotent close and use-after-release tested in lifecycle case.',
    'Dataset.stats':'No parameters; post-execution real diagnostic result, no new action by contract.',
    'Dataset.execution_metadata':'Internal diagnostic with no parameters; recorded after a real action.',
    'Dataset.size_bytes':'No parameters; checks actual positive byte count after real source execution.',
    'Dataset.schema':'Only boolean fetch_if_missing; positive verifies actual field order/types, no artificial unsupported argument.',
    'Dataset.columns':'Only boolean fetch_if_missing; positive verifies actual field order.',
}


def _run_case(fx,case,index,source):
    from demiflow.execution.run_context import RunContext
    from runtime_v3_support import _ExecuteActionRecorder, _RayDataBlocker
    root=fx.root/'api-runs'/('%03d-'%index+case.name)
    root.mkdir(parents=True)
    recorder=_ExecuteActionRecorder()
    outcome={'name':case.name,'apis':case.apis,'kind':case.kind,'variants':case.variants,'run_directory':str(root/'run')}
    observed=None
    try:
        with recorder, _RayDataBlocker() as blocker, (nullcontext(None) if case.self_managed else RunContext.start(root/'run',limits={'timeout_s':120,'max_input_rows':1000,'max_output_rows':4000,'checkpoint_rows':64})) as run:
            observed=run
            value=case.run(root)
            if case.error: raise AssertionError('Invalid API input was silently accepted')
            outcome['result']=value
        outcome['status']='passed'
    except Exception as exc:
        accepted=bool(case.error and isinstance(exc,case.error) and (case.match is None or re.search(case.match,str(exc),re.I)))
        outcome.update(status='passed' if accepted else 'failed',error={'type':type(exc).__name__,'message':str(exc)[:6000]})
        if not accepted: outcome['traceback']=traceback.format_exc(limit=12)
    outcome['execute_action_calls']=recorder.calls
    outcome['ray_data_import_attempts']=list(blocker.attempts)
    if case.requires_execution and case.kind == 'positive' and not recorder.calls:
        outcome['status']='failed'; outcome['missing_execution']=True
    if blocker.attempts: outcome['status']='failed'
    if observed is not None:
        with observed.journal._connection() as db:
            outcome['native_actions']=[{'action_id':a,'complete':result is not None} for a,result in db.execute('SELECT action_id,result FROM runtime_actions ORDER BY action_id')]
            outcome['native_commits']=[{'commit_id':cid,'status':s} for cid,s in db.execute('SELECT commit_id,status FROM runtime_commits ORDER BY sequence')]
        if outcome['status']=='passed' and case.kind=='positive' and case.requires_execution:
            if not outcome['native_commits'] or any(c['status']!='acked' for c in outcome['native_commits']):
                outcome['status']='failed'; outcome['unconfirmed_native_commit']=True
    if case.self_managed and outcome['status']=='passed':
        from demiflow.execution.run_journal import RunJournal
        scopes=outcome['result'].get('native_scopes', [])
        outcome['native_commits']=[]
        for scope in scopes:
            with RunJournal(scope)._connection() as db:
                outcome['native_commits'].extend({'scope':scope,'commit_id':cid,'status':status} for cid,status in db.execute('SELECT commit_id,status FROM runtime_commits'))
        if not outcome['native_commits'] or any(c['status']!='acked' for c in outcome['native_commits']):
            outcome['status']='failed';outcome['unconfirmed_native_commit']=True
    return outcome


def api_matrix(fx,fault=None):
    """Run all planned groups or an explicit group; return coverage without a false green."""
    from runtime_v3_recovery_scenarios import _provider
    plan=json.loads((WORKTREE/'docs/unified-runtime-v3-20261114/plan.json').read_text())['api']
    assert len(plan)==98 and len({x['symbol'] for x in plan})==98
    source=_fixtures(fx.root/'api-sources')
    cases=_cases(source)
    known={a['symbol'] for a in plan}
    assert all(set(c.apis)<=known for c in cases)
    groups={c.group for c in cases}
    if fault is not None and fault not in groups: raise ValueError('Unknown API group: '+fault)
    selected=[c for c in cases if fault is None or c.group==fault]
    outcomes=[]
    with _provider(fx.provider):
        for index,case in enumerate(selected):
            outcomes.append(_run_case(fx,case,index,source))
    mapping={}
    for api in plan:
        symbol=api['symbol']; reached=[c for c in outcomes if symbol in c['apis']]
        positive=[c for c in reached if c['kind']=='positive']; negative=[c for c in reached if c['kind']=='negative']
        failed=[c['name'] for c in reached if c['status']=='failed']
        retired=api['disposition']=='retired_preserved_rejection'
        missing=[]
        if not positive and not retired: missing.append('No positive execution in this selected matrix; not claimed.')
        if not negative and symbol not in NA_NEGATIVE: missing.append('No invalid-input execution in this selected matrix; not claimed.')
        status='failed' if failed else 'not_run' if missing else 'tested'
        mapping[symbol]={'status':status,'signature':api['signature'],'disposition':api['disposition'],
                         'positive':[c['name'] for c in positive],'negative':[c['name'] for c in negative],
                         'negative_not_applicable':NA_NEGATIVE.get(symbol),'not_run':missing,'failed':failed}
    report={'api_count':len(plan),'api':mapping,'cases':outcomes,
            'executed':sorted({a for c in outcomes for a in c['apis']}),
            'tested':{a:v for a,v in mapping.items() if v['status']=='tested'},
            'not_run':{a:v for a,v in mapping.items() if v['status']=='not_run'},
            'failed':{a:v for a,v in mapping.items() if v['status']=='failed'},
            'retired_rejected':[a for a,v in mapping.items() if v['status']=='tested' and v['disposition']=='retired_preserved_rejection'],
            'child_execute_action_calls':[x for c in outcomes for x in c['execute_action_calls']],
            'child_ray_data_import_attempts':[x for c in outcomes for x in c['ray_data_import_attempts']],
            'self_managed_run_context':True,'provider':fx.provider,'group':fault}
    report['complete']=not report['not_run'] and not report['failed']
    fx.evidence_dir.mkdir(parents=True,exist_ok=True)
    (fx.evidence_dir/'api-matrix.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str))
    return report



def _extra_cases(source):
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_runtime_v3_native_execution import PACK
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    import pyarrow as pa
    result=[]
    def pos(name,apis,fn,group='native'):
        result.append(Case(name,tuple(apis.split(',')),group,fn))
    def neg(name,api,fn,match,group='native'):
        result.append(Case(name,(api,),group,fn,(ValueError,TypeError,KeyError,DataflowError),match,False,'negative'))
    for sync in (True,False):
        pos('cache-'+str(sync),'Dataset.map_cached',lambda r,v=sync:_cache(r,v))
    neg('cache-invalid-capacity','Dataset.map_cached',lambda r:_ds().map_cached(_inc,cache_dir=r/'cache',version='v1',max_entries=0),'max_entries')
    pos('kv-lookup','Dataset.lookup',_kv_lookup)
    neg('lookup-invalid-concurrency','Dataset.lookup',lambda r:_ds().lookup(None,key='id',concurrency=0),'concurrency|lookup source')
    pos('durable-admission','Dataset.admit_rows',lambda r:_admission(r,False))
    pos('durable-deduplication','Dataset.deduplicate',lambda r:_admission(r,True))
    neg('admission-empty-key','Dataset.admit_rows',lambda r:_ds().admit_rows(path=r/'db',key=''),'fields must be nonempty')
    neg('dedup-empty-keys','Dataset.deduplicate',lambda r:_ds().deduplicate([],path=r/'db',key='id'),'deduplicate')
    pos('pinned-lookup','Dataset.lookup_lance',lambda r:_lance_lookup(source))
    neg('lookup-zero-matches','Dataset.lookup_lance',lambda r:_ds().lookup_lance({'uri':str(source/'rows.lance'),'version':1},on='id',max_matches=0),'Invalid lookup bounds')
    pos('prepend-order','Dataset.prepend',lambda r:_same([{'id':0}]+ROWS,_ds().prepend(data.from_items([{'id':0}]),max_rows=1).take_all()))
    neg('prepend-bound','Dataset.prepend',lambda r:_ds().prepend(data.from_items(ROWS),max_rows=1).take_all(),'max_rows')
    pos('native-stream-write','Dataset.save_lance,Dataset.run_stream',_save_stream)
    neg('save-invalid-batch','Dataset.save_lance',lambda r:_ds().save_lance(r/'out.lance',schema=pa.Table.from_pylist(ROWS).schema,key='id',stage='out',max_batch=0),'max_batch')
    neg('run-stream-negative-log','Dataset.run_stream',lambda r:_ds().map_async(_ainc).run_stream(log_every=-1),'log_every')
    pos('vector-native-source','vector_search_lance',lambda r:_vectors(source,False))
    pos('vector-per-row','Dataset.search_vectors',lambda r:_vectors(source,True))
    neg('vector-zero-k','vector_search_lance',lambda r:data.vector_search_lance(str(source/'vectors.lance'),[1.,0.],vector_column='vector',top_k=0),'top_k')
    neg('vector-row-zero-k','Dataset.search_vectors',lambda r:_ds().search_vectors(query='v',output='hits',uri=str(source/'vectors.lance'),vector_column='vector',columns=['id'],top_k=0),'top_k')
    for kind,api in [('prompt','Dataset.map_prompt'),('async','Dataset.map_prompt_async'),('embedding','Dataset.map_embeddings'),('agent','Dataset.agentmap_async')]:
        pos('model-'+kind,api,lambda r,k=kind:_model_case(r,k),group='models')
    neg('prompt-missing-definition','Dataset.map_prompt',lambda r:_ds().map_prompt('missing',config=parse_prompt_pack(PACK),inputs={'payload':'id'},output='answer'),'missing|prompt',group='models')
    neg('async-zero-concurrency','Dataset.map_prompt_async',lambda r:_ds().map_prompt_async('enrich',config=parse_prompt_pack(PACK),inputs={'payload':'id'},output='answer',concurrency=0),'concurrency',group='models')
    neg('embedding-zero-batch','Dataset.map_embeddings',lambda r:_embedding_bad(),'batch_size',group='models')
    neg('agent-missing-config','Dataset.agentmap_async',lambda r:_ds().agentmap_async('enrich',config=None,inputs={'payload':'id'},output='answer'),'config|AgentConfig',group='models')
    pos('receipt-source','read_document_receipts',_receipts)
    neg('receipt-wrong-sha','read_document_receipts',lambda r:_receipts(r,bad=True),'SHA256')
    pos('wiki-native','Dataset.parse_wikitext',_wiki)
    neg('wiki-character-bound','Dataset.parse_wikitext',lambda r:data.from_items([{'text':'[[A]]'}]).parse_wikitext(column='text',output='syntax',max_chars=1).take_all(),'max_chars|character')
    for family,apis in [('document','Dataset.read_object,Dataset.decode_document'),('image','Dataset.read_object,Dataset.encode_image')]:
        pos('object-'+family,apis,lambda r,f=family:_object(r,f))
    neg('object-invalid-limit','Dataset.read_object',lambda r:_ds().read_object(reference='ref',output='raw',root=str(r),max_bytes=0),'max_bytes')
    neg('decode-invalid-limit','Dataset.decode_document',lambda r:_ds().decode_document(column='raw',output='doc',max_blocks=0),'max_blocks')
    neg('encode-invalid-profile','Dataset.encode_image',lambda r:_ds().encode_image(column='raw',output='img',profile='unknown'),'profile')
    return result


def _cache(root,sync):
    from demiflow import data
    calls=root/'calls'
    def value(text):
        with calls.open('a') as out: out.write(text+'\n')
        return text.upper()
    async def avalue(text): return value(text)
    for number in (1,2):
        rows=data.from_items([{'text':'one','current':number}]).map_cached(value if sync else avalue,
            cache_dir=root/'cache',version='v1',synchronous=sync,inputs=['text'],output='answer').take_all()
        _same([{'text':'one','current':number,'answer':'ONE'}],rows)
    assert calls.read_text().splitlines()==['one']
    return {'provider_calls':len(calls.read_text().splitlines())}


def _kv_lookup(root):
    from demiflow import data
    from demiflow.kv import SQLiteKV
    store=SQLiteKV(root/'lookup.sqlite','owned',missing='empty')
    store.put_many([('a',{'ref':'value'})])
    rows=data.from_items([{'key':'a'},{'key':'missing'}]).lookup(store,key='key',output='found').take_all()
    return _same([{'key':'a','found':{'hit':True,'value':{'ref':'value'}}},{'key':'missing','found':{'hit':False,'value':None}}],rows)


def _admission(root,dedup):
    from demiflow import data
    rows=[{'id':'a','key':'same'},{'id':'b','key':'same'},{'id':'c','key':'other'}]
    ds=data.from_items(rows)
    ds=(ds.deduplicate('key',path=root/'admission.sqlite',key='id') if dedup else
        ds.admit_rows(path=root/'admission.sqlite',key='id',unique_on=['key']))
    out=ds.take_all()
    assert [r['id'] for r in out]==(['a','c'] if dedup else ['a','b','c'])
    if not dedup: assert [r['admission']['status'] for r in out]==['admitted','duplicate','admitted']
    return {'rows':out}


def _lance_lookup(source):
    from demiflow import data
    out=data.from_items([{'id':2},{'id':9}]).lookup_lance({'uri':str(source/'rows.lance'),'version':1},on='id',max_matches=1).take_all()
    return _same([{'id':2,'matches':[ROWS[0]]},{'id':9,'matches':[]}],out)


def _save_stream(root):
    from demiflow import data
    import pyarrow as pa
    import lance
    expected=[{'id':'a','value':1},{'id':'b','value':2}]
    stats=data.from_items(expected).save_lance(root/'stream.lance',schema=pa.Table.from_pylist(expected).schema,
        key='id',stage='owned',max_batch=1).run_stream()
    output=lance.dataset(**stats.outputs['owned']).to_table().to_pylist()
    _same(expected,output)
    return {'outputs':stats.outputs,'rows':len(output)}


def _vectors(source,per_row):
    from demiflow import data
    uri=str(source/'vectors.lance')
    if per_row:
        out=data.from_items([{'q':[1.,0.]},{'q':[0.,1.]}]).search_vectors(query='q',output='hits',
            uri=uri,version=1,vector_column='vector',columns=['id'],top_k=1,metric='l2').take_all()
        assert [r['hits'][0]['id'] for r in out]==[1,2]
        assert all(r['hits'][0]['_distance']==0 for r in out)
    else:
        out=data.vector_search_lance(uri,[1.,0.],version=1,vector_column='vector',columns=['id'],top_k=1,metric='l2').take_all()
        assert len(out)==1 and out[0]['id']==1 and out[0]['_distance']==0
    return {'rows':out}


def _model_case(root,kind):
    import os
    from demiflow import data
    from runtime_v3_recovery_scenarios import _http_fixture, _model_pipeline
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_runtime_v3_native_execution import PACK
    with _http_fixture(root) as (requests,url):
        old={key:os.environ.get(key) for key in ('DF_V3_NATIVE_TEST_URL','DF_V3_NATIVE_TEST_KEY')}
        os.environ['DF_V3_NATIVE_TEST_URL']=url+('/agent/v1' if kind=='agent' else '/v1')
        os.environ['DF_V3_NATIVE_TEST_KEY']='owned-local'
        try:
            if kind=='prompt':
                ds=data.from_items([{'item':'one'}]).map_prompt('enrich',config=parse_prompt_pack(PACK),inputs={'payload':'item'},output='answer',max_requests=1)
            else: ds=_model_pipeline('prompt' if kind=='async' else kind)
            out=ds.take_all()
            _same([{'item':'one','embedding':[1.,0.,0.]}] if kind=='embedding' else [{'item':'one','answer':'ok'}],out)
            assert len(requests)==1
            return {'rows':out,'http_requests':len(requests)}
        finally:
            for key,value in old.items():
                if value is None: os.environ.pop(key,None)
                else: os.environ[key]=value


def _embedding_bad():
    from demiflow.embeddings import EmbeddingModel
    return _ds().map_embeddings(model=EmbeddingModel('owned','v1',3,'http://127.0.0.1:1/v1'),inputs={'text':'k'},batch_size=0)


def _receipts(root,bad=False):
    from test_document_receipt_source import snapshot
    from demiflow import data
    path,digest=snapshot(root)
    out=data.read_document_receipts(str(path),sha256='b'*64 if bad else digest).take_all()
    assert [r['receipt']['status'] for r in out]==['ok','timeout']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    return {'rows':out,'source_unchanged':True}


def _wiki(root):
    from demiflow import data
    out=data.from_items([{'text':'[[A|label]] {{lang|en|word}}'}]).parse_wikitext(column='text',output='syntax').take_all()
    assert out[0]['text']=='[[A|label]] {{lang|en|word}}'
    assert any(n.get('name')=='A' for n in out[0]['syntax']['nodes'])
    return {'nodes':len(out[0]['syntax']['nodes'])}


def _object(root,family):
    from demiflow import data
    from test_collect_flow_contract import picture,document
    raw=picture() if family=='image' else json.dumps(document()).encode()
    path=root/'object';path.write_bytes(raw)
    ref={'uri':path.as_uri(),'sha256':hashlib.sha256(raw).hexdigest()}
    ds=data.from_items([{'ref':ref}]).read_object(reference='ref',output='raw',root=str(root))
    if family=='image':
        ds=ds.encode_image(column='raw',output='image',profile='oriented_white_bicubic_v1',max_side=17,memory_bytes=268435456)
    else: ds=ds.decode_document(column='raw',output='document')
    out=ds.take_all()
    assert out[0]['raw']==raw
    if family=='image': assert out[0]['image']['data_url'].startswith('data:image/jpeg;base64,')
    else:
        expected=document()
        _same({k:expected[k] for k in ('source','blocks')},out[0]['document'])
    return {'raw_bytes':len(raw),'processed':family}


def _integration_cases(source):
    from demiflow import data
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_prompt_packing_contract import definition,arguments
    cases=[]
    def pos(name,apis,group,run,managed=False):
        cases.append(Case(name,tuple(apis.split(',')),group,run,self_managed=managed))
    def neg(name,api,group,run,match):
        cases.append(Case(name,(api,),group,run,(ValueError,TypeError,DataflowError,RecoveryContractError),match,False,'negative'))
    pos('document-register','Dataset.register_documents','documents',_register_document)
    pos('document-register-batch','Dataset.register_documents','documents',_register_document_batch)
    neg('register-invalid-library','Dataset.register_documents','documents',lambda r:_ds().register_documents(request='r',output='out',library=None),'DocumentLibrary')
    pos('document-read','Dataset.read_documents','documents',_read_documents)
    neg('read-invalid-context','Dataset.read_documents','documents',lambda r:_ds().read_documents(request='r',output='out',context=None),'PromptContext')
    pos('document-embeddings','Dataset.document_embeddings','documents',_document_embeddings)
    neg('document-embedding-invalid-batch','Dataset.document_embeddings','documents',lambda r:_ds().document_embeddings(model=None,batch_size=0),'batch_size|model')
    pos('prompt-packing','Dataset.pack_prompt_inputs','documents',_packing)
    def bad_pack(root):
        args=arguments(max_items=0);prompt=args.pop('prompt');config=root/'prompt.yaml';config.write_text(definition());args['config']=str(config)
        return _ds().pack_prompt_inputs(prompt,**args)
    neg('pack-invalid-size','Dataset.pack_prompt_inputs','documents',bad_pack,'max_items')
    pos('native-search-http','Dataset.search_web','acquisition',_search)
    neg('search-empty-binding','Dataset.search_web','acquisition',lambda r:_ds().search_web(requests='',output='found',session=None),'nonempty|session')
    for explicit in (False,True):
        pos('queue-explicit-'+str(explicit),'Dataset.enqueue,read_queue,Dataset.fetch,Dataset.ack_queue,read_queue_records' if explicit else 'Dataset.enqueue,read_queue,Dataset.fetch,Dataset.write_datasink,read_queue_records',
            'queue',lambda r,e=explicit:_queue(r,e),True)
    pos('legacy-queue-archive','read_legacy_queue_records','queue',_legacy)
    neg('legacy-wrong-hash','read_legacy_queue_records','queue',lambda r:_legacy(r,True),'SHA256')
    from demiflow.execution.queue import QueueConfig
    def config(root): return QueueConfig(backend='sqlite',queue_id='owned',path=str(root/'queue.sqlite'))
    neg('enqueue-invalid-depth','Dataset.enqueue','queue',lambda r:_ds().enqueue(config(r),producer=None,queue_depth=0),'queue_depth|producer')
    neg('queue-zero-tasks','read_queue','queue',lambda r:data.read_queue(config(r),pool='p',max_tasks=0),'max_tasks')
    neg('queue-ack-depth','Dataset.ack_queue','queue',lambda r:_ds().ack_queue(config(r),queue_depth=0),'ACK queue depth|queue delivery')
    neg('history-invalid-boundary','read_queue_records','queue',lambda r:data.read_queue_records(config(r),pool='p',through=-1),'QueueSnapshot')
    neg('fetch-invalid-store','Dataset.fetch','queue',lambda r:_ds().fetch(store=None),'store')
    neg('datasink-invalid-adapter','Dataset.write_datasink','queue',lambda r:_ds().write_datasink(object()),'sink|adapter')
    return cases


def _register_document(root):
    from demiflow import data
    from demiflow.collect.document_library import DocumentLibrary
    request={'format':'text','text':'中文正文。\n\nSecond paragraph.',
        'source':{'url':'https://fixture.invalid/a','final_url':'https://fixture.invalid/a','title':'Owned','retrieved_at':''}}
    library=DocumentLibrary(index_path=root/'index.sqlite',object_directory=root/'objects')
    out=data.from_items([{'request':request}]).register_documents(request='request',output='registration',library=library).take_all()
    ref=out[0]['registration']['document_ref']
    assert out[0]['registration']['status']=='ok'
    assert library.lookup('https://fixture.invalid/a',max_bytes=1024,max_document_bytes=4096)['document_ref']==ref
    from demiflow.collect.documents import read_document
    document=read_document(ref)
    assert any('中文正文。' in block['text'] for block in document['blocks'])
    return {'registration':out[0]['registration'],'body_verified':True}


def _reading_inputs(row,reading): return {'payload':reading}


def _read_documents(root):
    from demiflow import data
    from demiflow.collect.reading import PromptContext,read_documents
    from demiflow.operator_llm.tokens import CharacterBudget
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_agentmap_async import resource,api_call,PACK
    request=api_call(resource.__wrapped__(root))['arguments']['request']
    context=PromptContext(parse_prompt_pack(PACK).prompt_definitions['enrich'],CharacterBudget(60000),_reading_inputs)
    expected=asyncio.run(read_documents(request,context=context))
    out=data.from_items([{'request':request}]).read_documents(request='request',output='reading',context=context,concurrency=1).take_all()
    _same(expected,out[0]['reading'])
    assert out[0]['reading']['status']=='ok' and out[0]['reading']['material_chars']>0
    return {'material_chars':out[0]['reading']['material_chars'],'prompt_chars':out[0]['reading']['prompt_chars']}


def _document_embeddings(root):
    from copy import deepcopy
    from demiflow import data
    from demiflow.embeddings import EmbeddingModel
    from test_document_embeddings import fixture_input
    from runtime_v3_recovery_scenarios import _http_fixture
    value,text=fixture_input(root)
    bad=deepcopy(value);bad['spans'][0]['end']=99999
    with _http_fixture(root) as (requests,url):
        model=EmbeddingModel('owned','fixed',3,url+'/v1')
        out=data.from_items([{'id':'ok','document_input':value},{'id':'bad','document_input':bad}]).document_embeddings(
            model=model,batch_size=2,concurrency=1,error_output='error',call_output='call').take_all()
        assert len(requests)==1 and requests[0]['body']['input']==[text]
        good=next(r for r in out if r['id']=='ok');invalid=next(r for r in out if r['id']=='bad')
        assert good['embedding']==[1.,0.,0.] and good['call']['input_text_sha256']==value['text_sha256']
        assert invalid['embedding'] is None and invalid['error']
    return {'http_requests':len(requests),'valid_rows':1,'invalid_rows':1}


def _packing(root):
    from demiflow import data
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_prompt_packing_contract import definition,arguments,row
    args=arguments();prompt=args.pop('prompt');config=root/'prompt.yaml';config.write_text(definition());args['config']=str(config)
    original=row()
    out=data.from_items([original]).pack_prompt_inputs(prompt,**args).take_all()
    expected=[{'start':0,'stop':2,'error':None},{'start':2,'stop':4,'error':None},{'start':4,'stop':5,'error':None}]
    _same({**original,'ranges':expected},out[0])
    return {'ranges':out[0]['ranges']}


def _search(root):
    import pytest
    from test_native_search_runtime import server,engines
    from test_runtime_v3_search_execution import _session,_dataset
    with contextmanager(server.__wrapped__)() as srv,pytest.MonkeyPatch.context() as patch:
        declared=engines.__wrapped__(srv,patch)
        ds=_dataset(_session(root,declared))
        out=ds.take_all()
        assert len(out)==1 and out[0]['found'][0]['status']=='ok' and out[0]['found'][0]['candidates']
        assert len(srv[1])==1
        metrics=ds.execution_metadata().diagnostics['resources']['WebSession:0']
        assert metrics['search_http_requests']==1
        return {'http_requests':len(srv[1]),'queries':metrics['search_queries']}


def _queue(root,explicit):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.queue import QueueChannel,QueueConfig,ProducerSourceSpec,task_message
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore,ObjectRef
    from test_raw_fetch import origin_server,BODY
    channel=QueueChannel(QueueConfig(backend='sqlite',queue_id='owned',path=str(root/'queue.sqlite')))
    producer=channel.open('p',producer_id='fixture',source=ProducerSourceSpec(kind='records',source_id='fixed',uri='memory:fixed',version=1))
    with origin_server() as (url,calls):
        with RunContext.start(root/'producer'):
            rows=[{'messages':[task_message(str(i),{'request':{'uri':url+'/file'}},pool='p')]} for i in range(2)]
            stats=data.from_items(rows).enqueue(channel.config,producer=producer).run_stream()
            assert stats.emitted==2
        assert channel.seal(producer)['actual_tasks']==2
        with RunContext.start(root/'consumer'):
            ds=data.read_queue(channel.config,pool='p',max_tasks=2,max_inflight=1,idle_timeout_s=.2,
                settlement='explicit' if explicit else 'sink').fetch(store=LocalObjectStore(root/'objects'),
                policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
            if explicit: ds.ack_queue(channel.config).run_stream()
            else: assert ds.write_datasink(QueueResultSink(channel.config,pool='p')) is None
        assert channel.state('p')['queue_drained'] and calls['/file']==2
        with RunContext.start(root/'history'):
            history=data.read_queue_records(channel.config,pool='p',through=channel.snapshot('p'),results=True).take_all()
            assert len(history)==2 and all(r['verification']['status']=='verified' for r in history)
            for row in history: assert ObjectRef(**row['result']['raw_ref']).read(max_bytes=len(BODY))==BODY
        return {'native_scopes':[str(root/name) for name in ('producer','consumer','history')],
                'http_requests':calls['/file'],'verified_history_rows':len(history),'explicit_settlement':explicit}


def _legacy(root,bad=False):
    from demiflow import data
    from test_sqlite_channel import archive
    path,digest=archive(root)
    for results in (False,True):
        out=data.read_legacy_queue_records(str(path),sha256='0'*64 if bad else digest,pool='p',through=1,results=results).take_all()
        assert len(out)==1 and out[0]['task_id']=='one' and out[0]['verification']['status']=='legacy_not_certified'
        assert ('old_status' in out[0]['value']) is results
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    assert not Path(str(path)+'-wal').exists()
    return {'archive_unchanged':True,'legacy_not_certified':True}


def _register_document_batch(root):
    from demiflow import data
    from demiflow.collect.document_library import DocumentLibrary
    from test_runtime_v3_document_batch import rows
    library=DocumentLibrary(index_path=root/'index.sqlite',object_directory=root/'objects')
    output=data.from_items(rows()).register_documents(request='request',output='registration',
        library=library,batch_size=2,concurrency=1,prepare_workers=1).take_all()
    assert len(output)==2 and all(r['registration']['status']=='ok' for r in output)
    for i,row in enumerate(output):
        assert library.lookup('https://fixture.invalid/'+str(i),max_bytes=1024,max_document_bytes=4096)['document_ref']==row['registration']['document_ref']
    return {'registered':len(output),'batch_size':2}
