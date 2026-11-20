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


def test_runtime_supplemental_platform_provenance_keeps_business_checks(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from demiflow.forge.development import resources
    from demiflow.forge.development.project import file_hash
    checkout=tmp_path/'checkout';root=checkout/'business';root.mkdir(parents=True)
    platform=checkout/'platform.md';business=root/'rules.md'
    platform.write_text('old platform');business.write_text('business')
    monkeypatch.setattr(resources,'PACKAGE_ROOT',checkout)
    monkeypatch.setattr(resources,'CHECKOUT_ROOT',checkout)
    project=SimpleNamespace(root=root)
    sources=[dict(canonical_path=str(p),sha256=file_hash(p)) for p in (platform,business)]
    platform.write_text('new platform')
    provenance=task_module._runtime_context_provenance(project,sources,None)
    assert set(provenance)=={str(platform)}
    assert task_module.supplemental_context(sources,_original_provenance=provenance)==sorted(sources,key=lambda v:v['canonical_path'])
    with pytest.raises(ForgeError,match='context changed'):
        task_module.supplemental_context(sources)
    business.write_text('changed business')
    with pytest.raises(ForgeError,match='context changed'):
        task_module.supplemental_context(sources,_original_provenance=provenance)


def test_runtime_workspace_projection_requires_authenticated_business_bytes(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from demiflow.forge.development import registration
    root=tmp_path/'business';root.mkdir()
    raw=json.dumps(dict(platform={'digest':'old'},locations={'data':'records'}))
    original=dict(after_raw=raw,after_sha256=registration.sha(raw))
    # The signing/chain validator is the established records boundary; this narrow
    # projection test supplies its authenticated result and tests byte/business proof.
    monkeypatch.setattr(registration,'records',lambda *a:[dict(status='committed',plan={'workspace':original})])
    (root/'workspace.json').write_text(json.dumps(dict(platform={'digest':'new'},locations={'data':'records'})))
    project=SimpleNamespace(root=root);state={'managed':{}}
    assert task_module._runtime_workspace(project,state)==original
    (root/'workspace.json').write_text(json.dumps(dict(platform={'digest':'new'},locations={'data':'other'})))
    with pytest.raises(ForgeError,match='business declaration changed'):
        task_module._runtime_workspace(project,state)
    (root/'workspace.json').write_text(raw)
    original['after_sha256']='0'*64
    with pytest.raises(ForgeError,match='business declaration changed'):
        task_module._runtime_workspace(project,state)
