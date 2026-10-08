import pytest
from demiflow import data


def collect(rows, path, **options):
    out = []
    (data.from_items(rows).admit_rows(path=path, key='id', unique_on=['group', 'url'],
        quotas=[{'on': [], 'limit': 3}, {'on': ['group'], 'limit': 2}], **options)
        .map(lambda row: out.append(row) or row).run_stream())
    return {r['id']: r['admission']['status'] for r in out}


def test_online_duplicate_quota_and_reordered_replay(tmp_path):
    rows = [dict(id=k, group=g, url=u) for k, g, u in
            [('a','x','u1'), ('b','x','u1'), ('c','x','u2'), ('d','x','u3'),
             ('e','y','u1'), ('f','z','u1')]]
    first = collect(rows, tmp_path/'admit.sqlite')
    assert first == dict(a='admitted', b='duplicate', c='admitted',
                         d='limited', e='admitted', f='limited')
    assert collect(rows[::-1], tmp_path/'admit.sqlite') == first


def test_reservation_survives_downstream_failure(tmp_path):
    path = tmp_path/'admit.sqlite'
    row = dict(id='first', group='x', url='u1')
    def fail(row):
        raise RuntimeError('sink failed')
    with pytest.raises(RuntimeError, match='sink failed'):
        (data.from_items([row]).admit_rows(path=path, key='id', quotas=[{'on': [], 'limit': 1}])
            .map(fail).run_stream())
    out = []
    (data.from_items([dict(id='new'), row]).admit_rows(path=path, key='id', quotas=[{'on': [], 'limit': 1}])
        .map(lambda r: out.append(r) or r).run_stream())
    assert [r['admission']['status'] for r in out] == ['limited', 'admitted']


def test_same_identity_in_one_stream_emits_once(tmp_path):
    out = []
    (data.from_items([{'id': 'a'}, {'id': 'a'}]).admit_rows(path=tmp_path/'a', key='id')
        .map(lambda r: out.append(r) or r).run_stream())
    assert [r['admission']['status'] for r in out] == ['admitted', 'duplicate']


def test_policy_and_identity_mismatch_are_not_rebudgeted(tmp_path):
    path = tmp_path/'a'
    collect([dict(id='a', group='x', url='1')], path)
    with pytest.raises(ValueError, match='different keys'):
        collect([dict(id='a', group='x', url='2')], path)
    with pytest.raises(ValueError, match='policy changed'):
        data.from_items([]).admit_rows(path=path, key='id').run_stream()


def test_admission_bounds_and_skipped_rows(tmp_path):
    with pytest.raises(ValueError, match='max_entries'):
        (data.from_items([{'id': 'a'}, {'id': 'b'}]).admit_rows(
            path=tmp_path/'a', key='id', max_entries=1).run_stream())
    with pytest.raises(ValueError, match='max_key_bytes'):
        (data.from_items([{'id': '宽'*8}]).admit_rows(
            path=tmp_path/'b', key='id', max_key_bytes=8).run_stream())
    out=[]
    (data.from_items([{'skip': True}, {'id': 'a'}]).admit_rows(path=tmp_path/'c', key='id',
            when=lambda r: not r.get('skip')).map(lambda r: out.append(r) or r).run_stream())
    assert [r['admission']['status'] for r in out] == ['skipped', 'admitted']


def test_quota_reader_observes_live_commits_without_reserving(tmp_path):
    from demiflow import AdmissionQuotaReader
    from demiflow.execution.stream_admission import StreamAdmission
    path = tmp_path/'quota.sqlite'
    quotas = [{'on': [], 'limit': 3}, {'on': ['group'], 'limit': 1}]
    reader = AdmissionQuotaReader(path, quotas=quotas)
    assert reader.remaining({'group': 'a'}) == [3, 1] and not path.exists()
    writer = StreamAdmission(path=path, key='id', unique_on=[], quotas=quotas, output='admission',
        when=None, max_entries=3, max_key_bytes=16384, max_disk_bytes=1024**2)
    writer.start()
    try:
        assert reader.remaining({'group': 'a'}) == [3, 1]
        writer.admit({'id': 'first', 'group': 'a'})
        assert reader.remaining({'group': 'a'}) == [2, 0]
        assert reader.remaining({'group': 'b'}) == [2, 1]
        assert writer.db.execute('SELECT count(*) FROM accepted').fetchone()[0] == 1
        with pytest.raises(ValueError, match='max_key_bytes'):
            reader.remaining({'group': 'x'*16385})
        changed = AdmissionQuotaReader(path, quotas=[{'on': [], 'limit': 100}])
        with pytest.raises(ValueError, match='reader policy changed'):
            changed.remaining({})
        assert changed.db is None
    finally:
        reader.close()
        import asyncio
        asyncio.run(writer.aclose())
    assert reader.db is None


def test_relax_limits_preserves_owners_counters_and_replay(tmp_path):
    from demiflow import AdmissionQuotaReader
    path = tmp_path/'relax.sqlite'
    def submit(items, limit, entries):
        output = []
        (data.from_items(items).admit_rows(path=path, key='id', unique_on=['url'],
            quotas=[{'on': [], 'limit': limit}], max_entries=entries)
            .map(lambda r: output.append(r) or r).run_stream())
        return [r['admission']['status'] for r in output]
    first = {'id': 'first', 'url': 'one'}
    second = {'id': 'second', 'url': 'two'}
    assert submit([first, second], 1, 1) == ['admitted', 'limited']
    # A reader may observe the relaxed allowance before the writer opens it.
    reader = AdmissionQuotaReader(path, quotas=[{'on': [], 'limit': None}])
    assert reader.remaining({}) == [None]
    reader.close()
    assert submit([{'id': 'copy', 'url': 'one'}, second, first], None, 3) == ['duplicate', 'admitted', 'admitted']
    assert submit([second, first], None, 3) == ['admitted', 'admitted']
    import sqlite3
    with sqlite3.connect(path) as db:
        assert db.execute('select count(*) from accepted').fetchone()[0] == 2
        assert db.execute('select n from counts').fetchone()[0] == 2
    with pytest.raises(ValueError, match='policy changed'):
        submit([], 4, 3)  # Unlimited cannot silently become finite again.


def test_relax_one_quota_retains_other_group_limit(tmp_path):
    path = tmp_path/'mixed.sqlite'
    def submit(items, total):
        output = []
        (data.from_items(items).admit_rows(path=path, key='id',
            quotas=[{'on': [], 'limit': total}, {'on': ['group'], 'limit': 1}])
            .map(lambda r: output.append(r) or r).run_stream())
        return [r['admission']['status'] for r in output]
    assert submit([{'id': 'a', 'group': 'x'}], 1) == ['admitted']
    assert submit([{'id': 'b', 'group': 'x'}, {'id': 'c', 'group': 'y'}], None) == ['limited', 'admitted']
