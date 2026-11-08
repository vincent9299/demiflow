"""One project conversation, native maintenance, retained raw history and bounded navigation."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys

import pytest
from test_forge_development_chat import configuration, wait_turn
from demiflow.workbench.conversations.service import Service
from demiflow.forge.schema import ForgeError


def test_singleton_concurrent_creations_and_legacy_read_only(tmp_path):
    root,path,_=configuration(tmp_path);service=Service(path)
    try:
        legacy=service.create('wfixture',root,'legacy','codex')['id']
        assert wait_turn(service,service.start('wfixture',root,legacy,'old',0,'Earlier decision')['id'])['state']=='completed'
        newer=service.create('wfixture',root,'newer','codex')['id']
        assert wait_turn(service,service.start('wfixture',root,newer,'new',0,'Current decision')['id'])['state']=='completed'
        empty=service.create('wfixture',root,'empty','codex')['id']
        assert service.conversation('wfixture',root)['session']['id']==newer
        with ThreadPoolExecutor(max_workers=4) as pool:
            created=list(pool.map(lambda n:service.create_project('wfixture',root,'tab-'+str(n),'codex')['id'],range(8)))
        assert set(created)=={newer}
        assert len(service.store.list('wfixture')['sessions'])==3
        with pytest.raises(ForgeError,match='read-only'):
            service.start_project('wfixture',root,legacy,'bad',1,'Do not send')
        assert service.store.show('wfixture',legacy)['revision']==1
        history=service.store.project_turns('wfixture',limit=1)
        assert history['next_offset']==1 and history['turns'][0]['session']==newer
        assert service.store.project_turns('wfixture',offset=1,limit=1)['turns'][0]['session']==legacy
        assert service.store.events('wfixture',legacy)['events']
    finally:service.close()
    service=Service(path)
    try:assert service.conversation('wfixture',root)['session']['id']==newer
    finally:service.close()


@pytest.mark.parametrize('provider',['codex','deepseek'])
def test_native_compaction_retains_history_and_continues_same_session(tmp_path,provider):
    root,path,value=configuration(tmp_path)
    if provider=='deepseek':
        fixture=Path(__file__).with_name('devchat_fake_acp.py');exe=tmp_path/'fake-acp'
        exe.write_text('#!'+sys.executable+'\nimport runpy\nrunpy.run_path('+repr(str(fixture))+',run_name="__main__")\n');exe.chmod(0o700)
        value['providers']={provider:dict(executable=str(exe),version='0.2.1-rc.2',model='fixture')};path.write_text(json.dumps(value))
    service=Service(path)
    if provider=='deepseek':
        original=service.capabilities
        def capabilities(*args):
            result=original(*args);result['providers'][0]['available']=True;return result
        service.capabilities=capabilities
    try:
        sid=service.create_project('wfixture',root,'create',provider)['id']
        with pytest.raises(ForgeError,match='No provider'):service.compact('wfixture',root,sid,'empty',0)
        first=service.start_project('wfixture',root,sid,'first',0,'Save this decision')
        assert wait_turn(service,first['id'])['state']=='completed'
        history=service.store.events('wfixture',sid)['events']
        native=service.store.session(sid)['provider_session']
        turn=service.compact('wfixture',root,sid,'archive',1)
        assert service.compact('wfixture',root,sid,'archive',1)['id']==turn['id']
        done=wait_turn(service,turn['id']);assert done['state']=='completed',done
        assert done['delivery']=='not_applicable'
        result=json.loads(done['result'])
        assert result['compaction']['status']=='compacted'
        assert service.store.session(sid)['provider_session']==native
        assert service.store.events('wfixture',sid)['events'][:len(history)]==history
        page=service.store.project_turns('wfixture')
        assert page['turns'][0]['operation']=='compact' and page['turns'][0]['compaction']['status']=='compacted'
        assert 'baseline' not in page['turns'][0] and 'context' not in page['turns'][0]
        if provider=='deepseek':
            (root/'compact-fail').touch()
            failed=wait_turn(service,service.compact('wfixture',root,sid,'failed-archive',2)['id'])
            assert failed['state']=='failed' and not json.loads(failed['result']).get('compaction')
        revision=service.store.session(sid)['revision']
        assert wait_turn(service,service.start_project('wfixture',root,sid,'continue',revision,'Continue with saved decision')['id'])['state']=='completed'
        assert service.store.session(sid)['provider_session']==native
    finally:service.close()
