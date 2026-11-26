"""Real prepared Flow basis uses business bindings across platform provenance drift."""
import copy
from pathlib import Path
import pytest
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_snapshot import _seal,write_snapshot
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as ops
from demiflow.forge.runs import manager as runs

from test_forge_runs import base,root,prepared,execute


def historical_platform(root):
    _,control,snapshot=ws.load_workspace(root)
    old=copy.deepcopy(snapshot)
    old['platform']['files'][0]['sha256']='a'*64
    old.pop('digests');_seal(old)
    path='.forge/platform/'+old['digests']['snapshot']+'.json'
    write_snapshot(root/path,old)
    expected=content_digest(control)
    control['platform']={'path':path,'digest':old['digests']['snapshot']}
    ws.write_control(root/'workspace.json',control,expected=expected)


def test_platform_drift_allows_new_prepare_start_and_original_basis(root):
    historical_platform(root)
    value=prepared(root)
    target,loaded=runs._load(root,value['run_id'])
    # Rebinding provenance after preparation cannot revoke an identical business basis.
    original=copy.deepcopy(loaded['manifest'])
    old=ws.load_workspace(root)[1]['platform']['digest']
    ws.bind_platform(root,expected=old)
    assert ops.report_status(root,original['report_digest'],platform_check=False)['status']=='checked'
    path=root/'flows/select_a.json'
    parameters=ws.read_control(path);unchanged=copy.deepcopy(parameters)
    expected=content_digest(parameters)
    parameters['arguments']['minimum']=0.99
    ws.write_control(path,parameters,expected=expected)
    with pytest.raises(ForgeError,match='basis is stale|parameters'):
        runs.start(root,value['run_id'],expected=value['digest'])
    assert runs.show(root,value['run_id'])['status']=='prepared'
    ws.write_control(path,unchanged,expected=content_digest(parameters))
    result=execute(root,value)
    assert result['status']=='succeeded',result
