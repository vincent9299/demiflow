"""Provider exit proofs stay resolvable inside deep real workspace paths."""
import json
from pathlib import Path
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


@pytest.fixture(autouse=True)
def _actual_tree():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize('mode',['thread','process'])
def test_real_deep_run_confirms_cleanup_with_bounded_resolvable_evidence(tmp_path,monkeypatch,mode):
    from demiflow.resources.thread import ThreadResourceProvider
    from demiflow.resources.local import LocalResourceProvider
    provider_class = ThreadResourceProvider if mode=='thread' else LocalResourceProvider
    absent = provider_class.ensure_absent
    proofs=[]
    def capture(provider, exact, deadline):
        result=absent(provider,exact,deadline)
        proofs.append((provider,dict(exact),dict(result)))
        return result
    monkeypatch.setattr(provider_class,'ensure_absent',capture)
    root=tmp_path/('project-'+'x'*90)/('workspace-'+'y'*90)/'.forge'/'runs'/'run'
    assert len(str(root))>256
    with RunContext.start(root), data.local_execution(workers=1,worker_mode=mode):
        assert data.from_items([{'n':0},{'n':1}]).map(lambda row:row).take_all()==[{'n':0},{'n':1}]
    assert proofs
    for provider,exact,proof in tuple(proofs):
        assert proof['cleanup']=='CONFIRMED' and len(proof['evidence_ref'].encode())<=256
        value=provider.resolve_evidence(exact['scope_id'],exact['allocation_id'],proof['evidence_ref'])
        if mode=='thread': assert value['cleanup']=='CONFIRMED'
        else: assert value['unit_stopped'] and value['birth_identity_gone'] and value['descendants_empty']
        assert provider.pool.snapshot()['charged']['cpu_millis']==0
        original=proof['evidence_ref']
        again=provider.ensure_absent(exact,1000)
        assert again['cleanup']=='CONFIRMED' and again['evidence_ref']==original
        assert provider.resolve_evidence(exact['scope_id'],exact['allocation_id'],original)==value


@pytest.mark.parametrize('mismatch',['scope','allocation','root','bytes','identity'])
def test_short_proof_rejects_wrong_domain_and_changed_original_bytes(tmp_path,mismatch):
    from demiflow.resources.evidence import evidence_reference,resolve_evidence
    from demiflow.resources.contracts import ResourceError
    root=tmp_path/'units';scope='scope';allocation='allocation'
    directory=root/scope/allocation;directory.mkdir(parents=True)
    original={'identity':{'pid':123,'start':'456','unit':{'device':1,'inode':2}}}
    (directory/'process.json').write_text(json.dumps(original))
    (directory/'cleanup.json').write_text(json.dumps({'unit_stopped':True,'birth_identity_gone':True,'descendants_empty':True}))
    ref=evidence_reference(root,scope,allocation,'cleanup.json')
    assert len(ref)<=256 and resolve_evidence(root,scope,allocation,ref)['unit_stopped']
    if mismatch=='scope':scope='other'
    if mismatch=='allocation':allocation='other'
    if mismatch=='root':root=tmp_path/'other'
    if mismatch=='bytes':(directory/'cleanup.json').write_text('{}')
    if mismatch=='identity':(directory/'process.json').write_text(json.dumps({'identity':{'pid':789}}))
    with pytest.raises(ResourceError,match='evidence'):
        resolve_evidence(root,scope,allocation,ref)


def test_absence_reference_does_not_survive_later_creation(tmp_path):
    from demiflow.resources.evidence import evidence_reference,resolve_evidence
    from demiflow.resources.contracts import ResourceError
    ref=evidence_reference(tmp_path,'scope','allocation','thread.json')
    assert resolve_evidence(tmp_path,'scope','allocation',ref) is None
    path=tmp_path/'scope'/'allocation'/'thread.json';path.parent.mkdir(parents=True)
    path.write_text('{}')
    with pytest.raises(ResourceError,match='evidence'):
        resolve_evidence(tmp_path,'scope','allocation',ref)


def test_legacy_released_path_evidence_is_preserved_on_repeated_cleanup(tmp_path,monkeypatch):
    import sys
    from demiflow.resources.process_unit import ManagedProcessUnit
    from demiflow.resources.local import LocalResourceProvider
    from demiflow.resources.capacity import LocalCapacityPool
    root=tmp_path/('workspace-'+'x'*100)/('run-'+'y'*100)
    pool=LocalCapacityPool(root/'pool',capacity={'cpu_millis':1000,'memory_bytes':4096})
    unit=ManagedProcessUnit(root/'units')
    provider=LocalResourceProvider(pool,unit)
    scope,allocation='scope','allocation'
    pool.reserve(allocation,'a'*64,{'cpu_millis':1000,'memory_bytes':1024},1000)
    spawned=unit.spawn(scope,allocation,'a'*64,(sys.executable,'-B','-c','import time;time.sleep(30)'))
    pool.bind_identity(allocation,spawned['identity'])
    proof=unit.stop(scope,allocation,terminate_deadline_ms=1000,process=spawned['process'])
    assert proof['confirmed']
    path=unit.directory(scope,allocation)/'cleanup.json'
    legacy=str(path)
    assert len(legacy)>256
    # An old provider really observed exit and released with its original path.
    pool.release(allocation,{'confirmed':True,'identity':spawned['identity'],'evidence_ref':legacy})
    original=path.read_bytes()
    result=provider.ensure_absent({'scope_id':scope,'allocation_id':allocation},1000)
    assert result['cleanup']=='CONFIRMED' and len(result['evidence_ref'])<=256
    assert path.read_bytes()==original
    assert json.loads((root/'pool'/'allocations'/f'{allocation}.json').read_bytes())['release_evidence']==legacy
    assert provider.resolve_evidence(scope,allocation,result['evidence_ref'])['unit_stopped']
    # A later uncertain observation cannot rewrite that historical proof or be
    # promoted to a new positive exit confirmation merely because one exists.
    from demiflow.resources import process_unit
    monkeypatch.setattr(process_unit,'unit_stopped',lambda *args:False)
    uncertain=unit.stop(scope,allocation,terminate_deadline_ms=1000)
    assert not uncertain['confirmed'] and path.read_bytes()==original
    assert uncertain['evidence_ref']==result['evidence_ref']


def test_provider_evidence_read_is_bounded(tmp_path):
    from demiflow.resources.evidence import evidence_reference
    from demiflow.resources.contracts import ResourceError
    path=tmp_path/'scope'/'allocation'/'thread.json';path.parent.mkdir(parents=True)
    path.write_bytes(b' '*8193)
    with pytest.raises(ResourceError,match='byte budget'):
        evidence_reference(tmp_path,'scope','allocation','thread.json')
