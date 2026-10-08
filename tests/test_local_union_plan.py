from demiflow import data
import pytest


class Parse:
    def __call__(self,row):
        return [{'key':row['value'],'n':1}]


@pytest.mark.parametrize('joined',[False,True])
@pytest.mark.parametrize('mode',['thread','process'])
def test_single_worker_class_callbacks_have_no_resident_actor_reservation(tmp_path,joined,mode):
    path=tmp_path/'lines.txt'
    path.write_text('a\nb\n')
    with data.local_execution(workers=1,partitions=2,worker_mode=mode):
        rows=data.read_records(str(path),format='text').flat_map(Parse)
        if joined: rows=rows.join(data.from_items([{'key':'b'}]),on='key',how='semi')
        result=rows.take_all()
    assert sorted(result,key=lambda r:r['key'])==([{'key':'b','n':1}] if joined else [{'key':'a','n':1},{'key':'b','n':1}])


def test_single_worker_union_keeps_class_transform_inside_same_engine(tmp_path):
    path=tmp_path/'lines.txt'
    path.write_text('a\nb\n')
    with data.local_execution(workers=1,partitions=2):
        left=data.from_items([{'key':'a','n':2}])
        right=data.read_records(str(path),format='text').flat_map(Parse)
        result=(left.union(right).reduce_by_key('key',lambda s,r:{'key':r['key'],'n':(s['n'] if s else 0)+r['n']}).take_all())
    assert sorted(result,key=lambda r:r['key'])==[{'key':'a','n':3},{'key':'b','n':1}]


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_union_preserves_child_order_with_keyed_and_lance_children(tmp_path, mode):
    import lance
    import pyarrow as pa
    uri = str(tmp_path/'source.lance')
    lance.write_dataset(pa.table({'key':['c','b','a'],'n':[3,2,1]}), uri, max_rows_per_file=2, max_rows_per_group=2)
    with data.local_execution(workers=2, partitions=2, batch_rows=1, worker_mode=mode):
        raw = data.read_lance(uri, version=1).map(lambda r:{**r,'kind':'raw'})
        keyed = data.read_lance(uri, version=1).reduce_by_key('key',lambda s,r:r).map(lambda r:{**r,'kind':'keyed'})
        # Also exercise a keyed child without a map that would clear its sorted flag.
        bare = data.read_lance(uri, version=1).reduce_by_key('key',lambda s,r:r)
        actual = raw.union(keyed).union(bare).take_all()
    assert [(r['key'],r.get('kind')) for r in actual] == [
        ('c','raw'),('b','raw'),('a','raw'),('a','keyed'),('b','keyed'),('c','keyed'),
        ('a',None),('b',None),('c',None)]


def test_union_rejects_unsupported_child_before_any_source_read(tmp_path):
    called = []
    def source():
        called.append('scan')
        yield {'key':'a'}
    with data.local_execution(workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        left = data.from_iter(source)
        right = data.from_iter(source).random_shuffle()
        with pytest.raises(NotImplementedError, match='RandomShuffleOp'):
            left.union(right).take_all()
    assert called == [] and not list(tmp_path.iterdir())


def test_many_plain_unions_do_not_allocate_identity_worker_layers(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow.execution.dataset_native import Engine
    from demiflow.data.sources import IterableSource
    original = Engine.python_rows
    def checked(self, executor, source, plan):
        if isinstance(source, IterableSource) and not plan.operations:
            raise AssertionError('Plain union allocated an identity worker layer')
        yield from original(self, executor, source, plan)
    monkeypatch.setattr(Engine, 'python_rows', checked)
    uri = str(tmp_path/'source.lance')
    lance.write_dataset(pa.table({'key':['a'], 'n':[1]}), uri)
    with data.local_execution(workers=2, partitions=2, temp_directory=str(tmp_path)):
        leaf = data.read_lance(uri, version=1)
        combined = leaf
        for _ in range(14):
            combined = combined.union(leaf)
        assert len(combined._source.inputs) == 15
        actual = combined.reduce_by_key('key', lambda s,r: {'key':r['key'], 'n':(s['n'] if s else 0)+r['n']}).take_all()
    assert actual == [{'key':'a', 'n':15}]


def test_union_flatten_keeps_intervening_transform_scope(tmp_path):
    with data.local_execution(workers=1, worker_mode='thread', temp_directory=str(tmp_path)):
        a = data.from_items([{'key':'a','n':1}])
        b = data.from_items([{'key':'b','n':2}])
        c = data.from_items([{'key':'c','n':3}])
        combined = a.union(b).map(lambda r:{**r,'n':r['n']*10}).union(c)
        assert combined.take_all() == [{'key':'a','n':10},{'key':'b','n':20},{'key':'c','n':3}]
