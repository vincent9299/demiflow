"""The public batched document actor reaches its actual preparation and index."""
from demiflow import data
from demiflow.collect.document_library import DocumentLibrary
from demiflow.execution.run_context import RunContext


def rows():
    return [{'request':{'format':'text','text':'owned text '+str(i),'source':{
        'url':'https://fixture.invalid/'+str(i),'final_url':'https://fixture.invalid/'+str(i),
        'title':'Owned','retrieved_at':''}}} for i in range(2)]


def test_real_batched_registration_uses_native_preparation_and_publication(tmp_path):
    library=DocumentLibrary(index_path=tmp_path/'index.sqlite',object_directory=tmp_path/'objects')
    with RunContext.start(tmp_path/'run') as run,data.local_execution():
        output=data.from_items(rows()).register_documents(request='request',output='registration',
            library=library,batch_size=2,concurrency=1,prepare_workers=1).take_all()
        assert len(output)==2 and all(r['registration']['status']=='ok' for r in output)
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
    for i,row in enumerate(output):
        original=library.lookup('https://fixture.invalid/'+str(i),max_bytes=1024,max_document_bytes=4096)
        assert original['document_ref']==row['registration']['document_ref']


def test_oversized_document_is_an_explicit_failed_row_without_library_entry(tmp_path):
    library=DocumentLibrary(index_path=tmp_path/'index.sqlite',object_directory=tmp_path/'objects')
    inputs=rows();inputs[1]['request']['text']='x'*1000
    with RunContext.start(tmp_path/'run'),data.local_execution():
        output=data.from_items(inputs).register_documents(request='request',output='registration',
            library=library,batch_size=2,concurrency=1,prepare_workers=1,max_bytes=128).take_all()
    assert [r['registration']['status'] for r in output]==['ok','invalid_document']
    assert library.lookup('https://fixture.invalid/0',max_bytes=1024,max_document_bytes=4096)
    assert library.lookup('https://fixture.invalid/1',max_bytes=1024,max_document_bytes=4096) is None
