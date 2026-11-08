"""Live-cell-bound complex source reads through real SQLite / staged authority.

Only the source-query RPC response is injected. There is no kernel, OCI or
Lance importer here; cancellation and permission checks use actual DB state.
"""
from demiflow.workbench.analysis import contracts as c
from test_forge_analysis_records import controller,document,source_manifest
from test_forge_analysis_sessions import PARENT,ready,submit
from test_forge_analysis_deferred_metadata import reply
from types import SimpleNamespace
import pytest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis.source_broker import SourceReads
from demiflow.workbench.analysis.source_broker import alias_binding
from test_forge_analysis_presentations import store,add_handle


def active_source(client):
    manifest=source_manifest();manifest['schema'][1]['type']='list'
    source=client.call('register_source',manifest=manifest)['result']
    client.call('remember_source',digest=source['digest'],locator=dict(kind='workspace_table',alias='fixed',binding=source['binding'],columns=['input','output']))
    book=client.call('create_notebook',request_id='book',document=document())['result']
    session=client.call('session_start',request_id='start',notebook_id=book['id'],revision=1,sources=dict(data=source['digest']),web_parent=PARENT)['result']
    execution=submit(client,book,session,'cell')['result'];ready(client,session)
    client.call('session_dispatch',session_id=session['id'],generation=session['generation'])
    return source,dict(session_id=session['id'],generation=session['generation'],execution_id=execution['id'])


def begin(client,scope,source,*,key='sample',digest=None):
    payload=dict(alias='data',digest=digest or source['digest'],query=dict(kind='sample',columns=['output'],limit=1))
    client.call('metadata_begin',key=key,name='source-sample',payload=payload,source_scope=scope,expected_principal='owner')
    value=reply(client,key,dict(principal='owner'))
    for _ in range(32):
        if value.get('code') or value['result']['done'] or value['result']['pending']=='source-query':return value
        assert not value['result']['in_transaction'] and not value['result']['locked']
        assert value['result']['pending']=='permission';value=reply(client,key,dict(allowed=True))
    raise AssertionError('source query not reached')


def response(source):
    return dict(kind='sample',source_snapshot=source['digest'],rows=[dict(_rowid='7',output=[dict(role='user',content='rich input')])],
        columns=['output'],offset=0,next_offset=1,total_rows=2,encoding='demiflow_preview_json_v1',complete=False)


def complete(client,key,value,*,allow=True):
    for _ in range(32):
        if value.get('code') or value['result']['done']:return value
        assert value['result']['pending']=='permission';value=reply(client,key,dict(allowed=allow))
    raise AssertionError('source completion not reached')


def test_complex_source_keeps_json_and_original_binding_and_no_sql_lock_during_rpc(tmp_path):
    with controller(tmp_path/'db') as client:
        source,scope=active_source(client);value=begin(client,scope,source)
        assert value['result']['payload']['purpose']=='notebook'
        assert value['result']['payload']['source_snapshot']==source['digest']
        value=complete(client,'sample',reply(client,'sample',response(source)))
        assert value['result']['value']['rows'][0]['output']==[dict(role='user',content='rich input')]
        assert begin(client,scope,source,key='wrong',digest='f'*64)['code']=='conflict'


def test_late_source_reply_after_cell_cancel_cannot_readopt_the_generation(tmp_path):
    with controller(tmp_path/'db') as client:
        source,scope=active_source(client);begin(client,scope,source)
        cancelled=client.call('session_cancel',request_id='cancel',execution_id=scope['execution_id'],expected_generation=scope['generation'])['result']
        assert cancelled['generation_will_stop']
        value=complete(client,'sample',reply(client,'sample',response(source)))
        assert value['code']=='conflict'


def test_last_source_read_grant_denial_or_foreign_snapshot_never_returns_values(tmp_path):
    with controller(tmp_path/'db') as client:
        source,scope=active_source(client);begin(client,scope,source)
        value=reply(client,'sample',response(source))
        assert complete(client,'sample',value,allow=False)['code']=='operation_rejected'
        begin(client,scope,source,key='foreign')
        foreign={**response(source),'source_snapshot':'e'*64}
        assert reply(client,'foreign',foreign)['code']=='conflict'


def test_generated_alias_validates_live_handle_reserved_names_cas_and_quota():
    with store() as (records,access,*unused):
        db=records.database.connection
        first=add_handle(db,701);second=add_handle(db,702)
        db.execute("UPDATE analysis_sessions SET generation='generation',state='executing'")
        db.execute("UPDATE analysis_executions SET state='running'");db.commit()
        authority=SimpleNamespace(access=lambda:access)
        reader=SourceReads(records,None,session_id='session',generation='generation',execution_id='execution',bindings={'numbers':first})
        assert reader.route(authority,'bind-table',dict(alias='numbers',handle=first))['id']==first['id']
        with pytest.raises(ForgeError,match='previous handle'):
            reader.route(authority,'bind-table',dict(alias='numbers',handle=second))
        assert reader.route(authority,'bind-table',dict(alias='numbers',handle=second,expected=first))['id']==second['id']
        assert reader.bindings=={'numbers':first} # Only Broker may commit the acknowledged binding.
        with pytest.raises(ForgeError,match='cannot be replaced'):
            reader.route(authority,'bind-table',dict(alias='data',handle=second))
        with pytest.raises(ForgeError):
            reader.route(authority,'bind-table',dict(alias='foreign',handle={**first,'digest':'0'*64}))
        with pytest.raises(ForgeError,match='at most16'):
            alias_binding({f't{i}':first for i in range(15)},['data'],dict(alias='new_name',handle=second))
        db.execute("UPDATE analysis_sessions SET state='stop_requested'");db.commit()
        with pytest.raises(ForgeError,match='live owning'):
            reader.route(authority,'bind-table',dict(alias='after_stop',handle=first))
