"""Batch publication preserves ordered conflict/idempotence and transaction rules."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import random
import sqlite3
import threading

import pytest

from demiflow.collect.document_library import DocumentLibrary
from demiflow.collect.documents import canonical
from demiflow.collect import document_index

URL='https://en.wikipedia.org/wiki/'
LIMITS={'max_bytes':100000,'max_document_bytes':100000}


def libraries(tmp_path):
    return [DocumentLibrary(index_path=tmp_path/(name+'.sqlite'),object_directory=tmp_path/'objects')
            for name in ('reference','batched')]


def body(library,name):
    return library.prepare({'format':'wikitext_sections',
        'source':{'url':URL+name,'final_url':URL+name,'title':name,'retrieved_at':''},
        'sections':[{'title':'Text','text':'Visible material '+name,'level':2}],
        'source_id':'wiki:'+name,'revision':'1'},**LIMITS)


def redirect(library,name,target,aliases=()):
    return library.prepare({'format':'redirect','url':URL+name,'target_url':URL+target,
        'aliases':[URL+a for a in aliases]},**LIMITS)


def ordinary(library,items):
    db=library._db()
    try:
        with db:
            db.execute('BEGIN IMMEDIATE')
            return [library._publish_one(db,item) for item in items]
    finally:db.close()


def tables(library):
    with sqlite3.connect(library.index_path) as db:
        return {table:db.execute('SELECT * FROM '+table+' ORDER BY 1,2').fetchall()
                for table in ('documents','urls','redirects')}


@pytest.mark.parametrize('window_rows',[1,7,256])
def test_batch_matches_ordered_publication_and_replay(tmp_path,monkeypatch,window_rows):
    monkeypatch.setattr(document_index,'_ROWS',window_rows)
    old,new=libraries(tmp_path)
    docs=[body(old,'body'+str(i)) for i in range(8)]
    seed=[docs[0],redirect(old,'known','body0',['known_alias'])]
    ordinary(old,seed);new.publish(seed)
    rng=random.Random(128)
    items=[]
    for i in range(320):
        if i%5==0:
            item=deepcopy(docs[rng.randrange(len(docs))])
            item['urls'].append(URL+'extra'+str(i))
            item['receipt']['acquisition']['registered_at']='later:'+str(i)
        elif i%13==0:
            item={'kind':'failure','receipt':{'status':'invalid_document','reason':'source_error','attempts':[]}}
        else:
            item=redirect(old,'alias'+str(rng.randrange(35)),'body'+str(rng.randrange(8)),
                ['known_alias' if i%7==0 else 'fresh'+str(i)])
        items.append(item)
    expected=ordinary(old,items)
    assert new.publish(items)==expected
    assert tables(new)==tables(old)
    # Replayed requests retain the original stored receipt, including aliases
    # first created by a different row; they do not acquire a new timestamp.
    assert new.publish(items)==ordinary(old,items)
    assert tables(new)==tables(old)


def test_failed_alias_group_has_no_partial_publication(tmp_path):
    library=libraries(tmp_path)[1]
    a=redirect(library,'shared','target_a')
    b=redirect(library,'new_b','target_b',['shared'])
    c=redirect(library,'new_c','target_c',['new_b'])
    result=library.publish([a,b,c])
    assert [r['status'] for r in result]==['redirect_registered','invalid_document','redirect_registered']
    with sqlite3.connect(library.index_path) as db:
        assert db.execute('SELECT target_url FROM redirects WHERE url=?',(URL+'new_b',)).fetchone()==(URL+'target_c',)


def test_metadata_and_key_budgets_preserve_results(tmp_path,monkeypatch):
    old,new=libraries(tmp_path)
    monkeypatch.setattr(document_index,'_KEYS',3)
    monkeypatch.setattr(document_index,'_BYTES',1024)
    items=[redirect(old,'wide','target',['alias'+str(i) for i in range(6)]),
        redirect(old,'small','target'),body(old,'body'),redirect(old,'next','target',['wide'])]
    assert new.publish(items)==ordinary(old,items)
    assert tables(new)==tables(old)


def test_large_existing_receipt_uses_no_batch_payload_cache(tmp_path,monkeypatch):
    old,new=libraries(tmp_path)
    item=redirect(old,'known','target')
    ordinary(old,[item]);new.publish([item])
    expanded={**item['receipt'],'legacy_padding':'x'*12000}
    for library in (old,new):
        with sqlite3.connect(library.index_path) as db:
            db.execute('UPDATE redirects SET receipt=?',(canonical(expanded),))
    monkeypatch.setattr(document_index,'_BYTES',8192)
    calls=[];original=DocumentLibrary._publish_one
    def observed(db,item):calls.append(item['kind']);return original(db,item)
    monkeypatch.setattr(DocumentLibrary,'_publish_one',staticmethod(observed))
    another=redirect(old,'new','target')
    expected=ordinary(old,[item,another]);calls.clear()
    assert new.publish([item,another])==expected
    assert calls==['redirect','redirect']
    assert tables(new)==tables(old)


def test_late_sql_error_rolls_back_all_windows(tmp_path,monkeypatch):
    library=libraries(tmp_path)[1]
    monkeypatch.setattr(document_index,'_ROWS',1)
    library._db().close()
    with sqlite3.connect(library.index_path) as db:
        db.execute("CREATE TRIGGER reject_last BEFORE INSERT ON redirects "
            "WHEN NEW.url LIKE '%last' BEGIN SELECT RAISE(ABORT,'injected storage rejection'); END")
    with pytest.raises(sqlite3.IntegrityError,match='injected'):
        library.publish([body(library,'body'),redirect(library,'first','body'),redirect(library,'last','body')])
    assert all(not rows for rows in tables(library).values())


def test_competing_publishers_preserve_alias_atomicity(tmp_path):
    library=libraries(tmp_path)[1]
    library._db().close();ready=threading.Barrier(2)
    batches=[[redirect(library,'shared','target'+str(i),['only'+str(i)])] for i in range(2)]
    def run(batch):ready.wait(timeout=5);return library.publish(batch)[0]
    with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(run,batches))
    assert sorted(r['status'] for r in results)==['invalid_document','redirect_registered']
    winner=next(i for i,r in enumerate(results) if r['status']=='redirect_registered')
    with sqlite3.connect(library.index_path) as db:
        assert db.execute('SELECT url,target_url FROM redirects ORDER BY url').fetchall()==[
            (URL+'only'+str(winner),URL+'target'+str(winner)),(URL+'shared',URL+'target'+str(winner))]
