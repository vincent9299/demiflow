"""Original trusted acceptance remains required after tool-only changes."""
import copy
import json
from pathlib import Path
import pytest
from demiflow.forge.development import project as project_module,tasks as task_module
from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import admission,sandbox
from test_forge_development_admission import admitted,base,root


def test_require_uses_original_signed_business_after_tool_and_sandbox_change(admitted,monkeypatch):
    f=admitted;before=admission.check(f.root)
    real=project_module.binding_components
    def changed(*args,**kwargs):
        parts=real(*args,**kwargs);parts['tool']={**parts['tool'],'new-platform.py':'a'*64};return parts
    monkeypatch.setattr(project_module,'binding_components',changed)
    monkeypatch.setattr(sandbox,'execution_identity',lambda:dict(backend='linux-user-pid-net-landlock-seccomp-v1',implementation='changed'))
    assert f.tasks.status(f.session['id'])['ready'] is False
    assert admission.check(f.root)==before
    # Missing old separated config evidence is a concrete proof gap; accepted facts survive.
    project=f.tasks._project()
    with task_module.locked(project,f.session['id']) as (_,state):
        saved=state.pop('binding_components')
    result=f.tasks.runtime_acceptance(f.session['id'])
    assert not result['ready'] and any('independent project configuration evidence' in v for v in result['violations'])
    with task_module.locked(project,f.session['id']) as (_,state):state['binding_components']=saved
    # A validly shaped changed business configuration cannot reuse acceptance.
    path=f.root/'.demiforge/project.json';original=path.read_bytes()
    value=json.loads(original);value['pipelines']['orders']['verify'][0]['argv'][-1]='changed'
    path.write_text(json.dumps(value))
    with pytest.raises(ForgeError):admission.check(f.root)
    path.write_bytes(original)
    assert admission.check(f.root)==before
