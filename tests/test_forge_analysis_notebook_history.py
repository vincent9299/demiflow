"""Real SQLite history lookup, without kernel/process execution."""
import pytest

from demiflow.workbench.analysis.sessions import Sessions
from demiflow.workbench.analysis.records import Access
from demiflow.forge.schema import ForgeError
from test_forge_analysis_presentations import store


def test_reopened_notebook_discovers_old_generation_without_execution_or_relabeling():
    with store() as (records,access,witness,raw,source,descriptor):
        sessions=Sessions(records);db=records.database.connection
        before=db.execute('SELECT * FROM analysis_executions').fetchall()
        listed=sessions.notebook_sessions(access,'book')
        assert len(listed['items'])==1 and listed['items'][0]['id']=='session'
        generations=sessions.generations(access,'session')
        assert [item['generation'] for item in generations['items']]==['new-live-generation','generation']
        assert generations['items'][0]['execution_count']==0 and generations['items'][0]['current']
        assert generations['items'][1]['execution_count']==1 and not generations['items'][1]['current']
        assert sessions.page(access,session_id='session',generation='generation')['items'][0]['output_id']==witness['artifact']['id']
        assert db.execute('SELECT * FROM analysis_executions').fetchall()==before
        assert db.execute('SELECT state FROM analysis_sessions').fetchone()[0]=='idle'


def test_history_does_not_cross_project_or_revoked_membership():
    with store() as (records,access,*_):
        sessions=Sessions(records)
        foreign=Access('w'+'9'*32,'other',lambda *args:True)
        for name,identifier in [('notebook_sessions','book'),('generations','session')]:
            with pytest.raises(ForgeError):getattr(sessions,name)(foreign,identifier)
            with pytest.raises(ForgeError):getattr(sessions,name)(Access(access.project,access.principal,lambda *args:False),identifier)
