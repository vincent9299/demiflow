import gzip
import json
import pytest
from demiflow.standalone import local_data
from demiflow.data.sources import DatasourceSource


def test_located_gzip_invalid_rows_and_json_container(tmp_path):
    p=tmp_path/'rows.jsonl.gz'
    with gzip.open(p,'wb') as f:f.write(b'{"id":1}\ninvalid\n\xff\n{"id":4}\n')
    data=local_data();ds=data.read_records(p)
    assert isinstance(ds._source,DatasourceSource)
    rows=ds.take_all()
    assert [r['row'] for r in rows]==[1,2,3,4]
    assert rows[0]['value']=={'id':1} and rows[1]['error'] and rows[2]['error']
    assert rows[3]['value']=={'id':4}
    assert len(data.read_records(p,max_records=2).take_all())==2
    array=tmp_path/'data.json';array.write_text('{"concepts":[{"name":"a"},{"name":"b"}]}')
    assert [r['value'] for r in data.read_records(array,format='json',item_prefix='concepts.item').take_all()]==[{'name':'a'},{'name':'b'}]
    array.write_text('{"concepts":[{"name":"a"},')
    with pytest.raises(__import__('ijson').JSONError):data.read_records(array,format='json',item_prefix='concepts.item').take_all()


def test_native_union_is_lazy_repeatable_and_keeps_context(tmp_path):
    data=local_data();seen=[]
    def rows():seen.append('read');yield {'id':1}
    left=data.from_iter(rows);ds=left.union(data.from_items([{'id':2}]))
    assert seen==[] and ds._executor is left._executor
    assert ds.take_all()==[{'id':1},{'id':2}]
    assert ds.take_all()==[{'id':1},{'id':2}] and len(seen)==2
    saved=ds.checkpoint(tmp_path/'union.jsonl',version='1')
    ds.checkpoint(tmp_path/'union.jsonl',version='1')
    assert len(seen)==3 and saved.count()==2


def test_scan_report_records_limit_errors_and_missing(tmp_path):
    p=tmp_path/'rows.jsonl';p.write_text('{"a":1}\nbad\n{"a":3}\n')
    report=tmp_path/'report.json'
    rows=local_data().read_records(p,max_records=2,report_path=report).take_all()
    scope=json.loads(report.read_text())
    assert len(rows)==2 and scope['rows']==2 and scope['invalid_rows']==1
    assert scope['status']=='budget_limited' and not scope['complete']
    missing=tmp_path/'missing.json'
    assert local_data().read_records(tmp_path/'absent',missing='empty',report_path=missing).take_all()==[]
    assert json.loads(missing.read_text())['status']=='missing'


def test_reader_detects_changed_file_and_preserves_failed_scope(tmp_path):
    from demiflow.data.records import iter_file_records
    p=tmp_path/'rows.jsonl';p.write_text('{"a":1}\n{"a":2}\n')
    report=tmp_path/'report.json';reader=iter_file_records(p,report_path=report)
    next(reader)
    p.write_text('{"a":1}\n{"a":20}\n')
    with pytest.raises(ValueError,match='changed'):list(reader)
    assert json.loads(report.read_text())['status']=='read_error'


def test_interrupted_scan_can_resume_without_losing_report(tmp_path):
    from demiflow.data.records import iter_file_records
    p=tmp_path/'rows.jsonl';p.write_text('{"a":1}\n{"a":2}\n')
    report=tmp_path/'report.json';reader=iter_file_records(p,report_path=report)
    next(reader);reader.close()
    assert json.loads(report.read_text())['status']=='interrupted'
    assert len(list(iter_file_records(p,report_path=report)))==2
    assert json.loads(report.read_text())['complete']
    prior=list(tmp_path.glob('report.json.attempt-*.json'))
    assert len(prior)==1 and json.loads(prior[0].read_text())['status']=='interrupted'
