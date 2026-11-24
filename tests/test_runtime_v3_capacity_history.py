"""Long-lived original capacity ledger: preserve tombstones without exhausting admission."""
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
import hashlib
import json
import multiprocessing
from pathlib import Path

import pytest

from demiflow.resources.capacity import LocalCapacityPool, MAX_ACTIVE_RECORDS
from demiflow.resources.contracts import ResourceError

CAPACITY = {'cpu_millis': 64, 'memory_bytes': 65536}
EFFECTIVE = {'cpu_millis': 1, 'memory_bytes': 16}
SPEC = 'a' * 64


def _seed(root, *, released=1021, active=5, capacity=None):
    pool = LocalCapacityPool(root, capacity=capacity or CAPACITY)
    # Use the original public lifecycle to get the actual persistent shape;
    # replicate only the owned test's already-final records without quadratic
    # setup scans. These fixtures never touch a deployment/shared pool.
    pool.reserve('template', SPEC, EFFECTIVE, 9999999999999)
    pool.bind_identity('template', {'pid': 2147480000, 'start': 'test-birth', 'unit': 'test'})
    pool.release('template', {'confirmed': True, 'identity': pool.reservation_view('template')['identity'],
                              'evidence_ref': 'original-confirmed-exit'})
    template = json.loads((root/'allocations'/'template.json').read_text())
    (root/'allocations'/'template.json').unlink()
    originals = {}
    for number in range(released + active):
        key = ('released-' + str(number)) if number < released else ('active-' + str(number-released))
        record = deepcopy(template); record['allocation_id'] = key
        if number >= released:
            record['state'] = 'BOUND'; record.pop('release_evidence'); record.pop('released_ms')
        raw = json.dumps(record, indent=2).encode() + b'\n'
        (root/'allocations'/(key+'.json')).write_bytes(raw)
        originals[key] = raw
    return pool, originals


def _archive_path(root, key):
    return root/'released'/hashlib.sha256(key.encode()).hexdigest()[:2]/(key+'.json')


def _assert_original_bytes(root, originals):
    for key, raw in originals.items():
        paths = [path for path in (root/'allocations'/(key+'.json'), _archive_path(root, key)) if path.exists()]
        assert paths, key
        assert all(path.read_bytes() == raw for path in paths), key
        if key.startswith('active-'): assert paths == [root/'allocations'/(key+'.json')]


def test_original_1026_layout_admits_new_work_without_losing_release_history(tmp_path):
    root = tmp_path/'pool'; pool, originals = _seed(root)
    view = pool.reserve('new', SPEC, EFFECTIVE, 9999999999999)
    assert view['state'] == 'RESERVED'
    snapshot = pool.snapshot()
    assert snapshot['charged'] == {'cpu_millis': 6, 'memory_bytes': 96}
    assert len(snapshot['active']) == 6
    assert any((root/'released').glob('*/*.json'))
    _assert_original_bytes(root, originals)
    fresh = LocalCapacityPool(root, capacity=CAPACITY)
    for key in ('released-0', 'released-1020'):
        assert fresh.reservation_view(key)['state'] == 'RELEASED'
        with pytest.raises(ResourceError, match='released allocation cannot reserve again'):
            fresh.reserve(key, SPEC, EFFECTIVE, 9999999999999)
        assert fresh.release(key, {'confirmed': True, 'identity': json.loads(originals[key])['identity'],
                                  'evidence_ref': 'original-confirmed-exit'})['idempotent']
    _assert_original_bytes(root, originals)


def test_released_history_leaves_the_live_directory_and_unknown_cleanup_stays_charged(tmp_path):
    root=tmp_path/'pool'; pool, originals = _seed(root, released=4, active=2)
    before = {key: (root/'allocations'/(key+'.json')).stat().st_ino for key in originals}
    with pytest.raises(ResourceError, match='capacity stays charged'):
        pool.release('active-0', {'confirmed': False, 'identity': None, 'evidence_ref': None})
    assert pool.snapshot()['charged'] == {'cpu_millis': 2, 'memory_bytes': 32}
    # The complete scan moved every released record to history: same bytes, same inode.
    assert sorted(path.name for path in (root/'allocations').glob('*.json')) == ['active-0.json', 'active-1.json']
    _assert_original_bytes(root, originals)
    assert before == {key: (root/'allocations'/(key+'.json') if key.startswith('active-')
                            else _archive_path(root, key)).stat().st_ino for key in originals}
    fresh = LocalCapacityPool(root, capacity=CAPACITY)
    for key in ('released-0', 'released-3'):
        assert fresh.reservation_view(key)['state'] == 'RELEASED'
        with pytest.raises(ResourceError, match='released allocation cannot reserve again'):
            fresh.reserve(key, SPEC, EFFECTIVE, 9999999999999)


def test_each_scan_moves_a_bounded_number_of_released_records(tmp_path):
    from demiflow.resources.capacity import MAX_ARCHIVED_PER_SCAN
    root=tmp_path/'pool'; pool, originals = _seed(root, released=2*MAX_ARCHIVED_PER_SCAN+5, active=2)
    live = lambda: len(list((root/'allocations').glob('*.json')))
    for remaining in (MAX_ARCHIVED_PER_SCAN+7, 7, 2, 2):
        assert pool.snapshot()['charged'] == {'cpu_millis': 2, 'memory_bytes': 32}
        assert live() == remaining
    _assert_original_bytes(root, originals)


@pytest.mark.parametrize('fault', ['after_link', 'after_target_sync', 'after_unlink'])
def test_crash_during_history_move_preserves_original_bytes_and_retry(tmp_path, monkeypatch, fault):
    import os
    root=tmp_path/'pool'; pool, originals = _seed(root)
    tripped=[]
    with monkeypatch.context() as patch:
        if fault == 'after_link':
            old=os.link
            def crash(*args, **kwargs):
                result=old(*args, **kwargs)
                if not tripped: tripped.append(True); raise OSError('owned crash after archive link')
                return result
            patch.setattr(os, 'link', crash)
        elif fault == 'after_unlink':
            old=Path.unlink
            def crash(path, *args, **kwargs):
                result=old(path,*args,**kwargs)
                if path.parent == root/'allocations' and not tripped:
                    tripped.append(True); raise OSError('owned crash after source unlink')
                return result
            patch.setattr(Path, 'unlink', crash)
        else:
            old=pool._sync_directory
            def crash(path):
                result=old(path)
                if path.parent == root/'released' and any(path.glob('*.json')) and not tripped:
                    tripped.append(True); raise OSError('owned crash after target fsync')
                return result
            patch.setattr(pool, '_sync_directory', crash)
        with pytest.raises(OSError, match='owned crash'):
            pool.reserve('never-admitted', SPEC, EFFECTIVE, 9999999999999)
    assert tripped and not (root/'allocations'/'never-admitted.json').exists()
    _assert_original_bytes(root, originals)
    fresh=LocalCapacityPool(root,capacity=CAPACITY)
    fresh.reserve('retry', SPEC, EFFECTIVE, 9999999999999)
    assert fresh.snapshot()['charged'] == {'cpu_millis': 6, 'memory_bytes': 96}
    _assert_original_bytes(root, originals)


def test_different_archive_copy_fails_closed_without_overwriting_either(tmp_path):
    root=tmp_path/'pool'; pool, originals=_seed(root)
    target=_archive_path(root,'released-0');target.parent.mkdir(parents=True)
    altered=json.loads(originals['released-0']);altered['release_evidence']='different-exit'
    raw=json.dumps(altered).encode();target.write_bytes(raw)
    with pytest.raises(ResourceError, match='conflicts'):
        pool.reserve('new',SPEC,EFFECTIVE,9999999999999)
    assert target.read_bytes()==raw
    assert (root/'allocations'/'released-0.json').read_bytes()==originals['released-0']
    assert not (root/'allocations'/'new.json').exists()


def test_large_legacy_history_makes_bounded_progress_without_partial_admission(tmp_path, monkeypatch):
    import os
    root=tmp_path/'pool';pool,originals=_seed(root,released=3*MAX_ACTIVE_RECORDS+17,active=3)
    old=os.scandir;counts=[]
    class Counting:
        def __init__(self,path):self.inner=old(path);self.count=0;self.path=Path(path)
        def __enter__(self):return self
        def __exit__(self,*args):self.inner.close();counts.append((self.path,self.count))
        def __iter__(self):return self
        def __next__(self):value=next(self.inner);self.count+=1;return value
    with monkeypatch.context() as patch:
        patch.setattr(os,'scandir',Counting)
        with pytest.raises(ResourceError, match='compaction made progress') as exc:
            pool.reserve('later',SPEC,EFFECTIVE,9999999999999)
    assert exc.value.code=='CAPACITY_PENDING'
    assert len(counts)==2 and all(n<=MAX_ACTIVE_RECORDS+1 for _,n in counts)
    assert not (root/'allocations'/'later.json').exists()
    _assert_original_bytes(root,originals)
    pool.reserve('later',SPEC,EFFECTIVE,9999999999999)
    assert pool.snapshot()['charged']=={'cpu_millis':4,'memory_bytes':64}
    _assert_original_bytes(root,originals)


def test_archive_never_makes_1024_live_allocations_fit_a_1024_slot_limit(tmp_path):
    capacity={'cpu_millis':100000,'memory_bytes':1000000}
    root=tmp_path/'pool';pool,originals=_seed(root,released=1,active=MAX_ACTIVE_RECORDS,capacity=capacity)
    with pytest.raises(ResourceError, match='active capacity records exceed budget|active scan budget'):
        pool.reserve('overflow',SPEC,EFFECTIVE,9999999999999)
    _assert_original_bytes(root,originals)
    assert not (root/'allocations'/'overflow.json').exists()


def _reserve_process(root,key):
    pool=LocalCapacityPool(root,capacity={'cpu_millis':8,'memory_bytes':128})
    try:return pool.reserve(key,SPEC,EFFECTIVE,9999999999999)['state']
    except ResourceError as exc:return exc.code


def test_concurrent_compaction_and_reservations_keep_single_original_charge(tmp_path):
    root=tmp_path/'pool';pool,originals=_seed(root,capacity={'cpu_millis':8,'memory_bytes':128})
    with ProcessPoolExecutor(max_workers=6,mp_context=multiprocessing.get_context('spawn')) as workers:
        results=list(workers.map(_reserve_process,[str(root)]*6,['parallel-'+str(i) for i in range(6)]))
    assert results.count('RESERVED')==3 and results.count('CAPACITY_PENDING')==3
    assert pool.snapshot()['charged']=={'cpu_millis':8,'memory_bytes':128}
    _assert_original_bytes(root,originals)


def test_archive_directory_symlink_is_rejected_without_writing_outside_pool(tmp_path):
    root=tmp_path/'pool';pool,_=_seed(root)
    outside=tmp_path/'outside';outside.mkdir();(root/'released').symlink_to(outside,target_is_directory=True)
    with pytest.raises(ResourceError,match='archive directory is a symlink'):
        pool.reserve('new',SPEC,EFFECTIVE,9999999999999)
    assert list(outside.iterdir())==[]


DEVICES = {'devices': [{'device_id': 'gpu0', 'type': 'gpu', 'memory_bytes': 1 << 30}]}
GPU = [{'type': 'gpu', 'count': 1}]


def _device_cycle(pool, key):
    pool.reserve(key, SPEC, GPU, deadline_ms=1000)
    pool.release(key, {'confirmed': True, 'identity': None, 'evidence_ref': 'exit:' + key})


def test_device_ledger_has_no_lifetime_record_limit_and_keeps_release_history(tmp_path):
    from demiflow.resources.device_pool import DevicePool
    root = tmp_path/'devices'; pool = DevicePool(root, inventory=DEVICES)
    for number in range(MAX_ACTIVE_RECORDS + 40):
        _device_cycle(pool, 'job-' + str(number))
    assert pool.snapshot()['active'] == []
    assert len(list((root/'allocations').glob('*.json'))) <= 1
    fresh = DevicePool(root, inventory=DEVICES)
    for key in ('job-0', 'job-' + str(MAX_ACTIVE_RECORDS + 39)):
        assert fresh.reservation_view(key) is None
        assert fresh.released_device_seconds(key, SPEC, 'gpu') is not None
        with pytest.raises(ResourceError, match='released allocation cannot reserve again'):
            fresh.reserve(key, SPEC, GPU, deadline_ms=1000)
        assert fresh.release(key, {'confirmed': True, 'identity': None, 'evidence_ref': 'exit:' + key})['idempotent']
    with pytest.raises(ResourceError, match='lacks matching evidence'):
        fresh.release('job-0', {'confirmed': True, 'identity': None, 'evidence_ref': 'another-exit'})


def test_device_ledger_charges_live_records_while_history_moves(tmp_path):
    from demiflow.resources.device_pool import DevicePool
    root = tmp_path/'devices'; pool = DevicePool(root, inventory=DEVICES)
    pool.reserve('holder', SPEC, GPU, deadline_ms=1000)
    template = json.loads((root/'allocations'/'holder.json').read_text())
    for number in range(2 * MAX_ACTIVE_RECORDS + 30):
        record = {**template, 'allocation_id': 'old-' + str(number), 'state': 'RELEASED',
                  'release_evidence': 'exit', 'released_ms': template['created_ms'] + 1}
        (root/'allocations'/('old-' + str(number) + '.json')).write_text(json.dumps(record))
    # The legacy directory exceeds two scan pages: bounded progress, never a partial admission.
    with pytest.raises(ResourceError) as error:
        pool.reserve('waiting', SPEC, GPU, deadline_ms=1000)
    assert error.value.code == 'CAPACITY_PENDING' and 'compaction made progress' in error.value.reason
    with pytest.raises(ResourceError) as error:
        pool.reserve('waiting', SPEC, GPU, deadline_ms=1000)
    assert error.value.code == 'CAPACITY_PENDING' and 'no free gpu devices' in error.value.reason
    assert [view['allocation_id'] for view in pool.snapshot()['active']] == ['holder']
    assert pool.released_device_seconds('old-0', SPEC, 'gpu') == 0.001
