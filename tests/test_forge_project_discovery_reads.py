"""Multi-entry discovery shares source work without weakening freshness/budgets."""
import hashlib

from demiflow.workbench.projects import discovery as discovery


def test_many_entries_in_one_module_fit_the_source_budget(tmp_path, monkeypatch):
    source = tmp_path / 'recipe.py'
    source.write_text('#' + 'x' * (768 * 1024) + '\n' + ''.join(
        f'def flow_{n}(source):\n    return source\n' for n in range(8)))
    reads = []
    read = discovery.read_bounded

    def count(path, maximum):
        raw = read(path, maximum)
        reads.append((path, len(raw)))
        return raw

    monkeypatch.setattr(discovery, 'read_bounded', count)
    result = discovery.discover(tmp_path, {'entries': {
        f'recipe.py:flow_{n}': [] for n in range(8)}})
    assert len(result['entries']) == 8
    assert all(e['status'] == 'recognized' for e in result['entries'])
    assert {d['code'] for d in result['diagnostics']} == {'parameters_unbound'}
    assert sum(size for path, size in reads if path == source) == source.stat().st_size


def test_shared_module_preserves_each_symbol_and_duplicate_diagnostics(tmp_path):
    (tmp_path / 'recipe.py').write_text(
        'def duplicate():\n    pass\n'
        'async def alternate():\n    pass\n'
        'def duplicate():\n    pass\n')
    result = discovery.discover(tmp_path, {'entries': {
        'recipe.py:duplicate': [], 'recipe.py:alternate': [], 'recipe.py:missing': []}})
    entries = {e['symbol']: e for e in result['entries']}
    assert entries['duplicate']['status'] == entries['missing']['status'] == 'invalid'
    assert entries['alternate']['status'] == 'recognized'
    assert entries['alternate']['line'] == 3


def test_changed_source_cannot_reuse_earlier_entry_facts(tmp_path, monkeypatch):
    source = tmp_path / 'recipe.py'
    source.write_text('def build():\n    pass\n')
    old = hashlib.sha256(source.read_bytes()).hexdigest()
    read = discovery.read_bounded

    def change_between_declarations(path, maximum):
        raw = read(path, maximum)
        if path == source:
            source.write_text('def changed():\n    pass\n')
        return raw

    monkeypatch.setattr(discovery, 'read_bounded', change_between_declarations)
    result = discovery.discover(tmp_path, {'entries': {'recipe.py:build': []}})
    assert result['status'] == 'partial'
    assert any(d['code'] == 'entry_unreadable' and 'changed' in d['detail'].lower()
               for d in result['diagnostics'])
    assert next(e for e in result['evidence'] if e['path'] == str(source))['sha256'] == old


def test_distinct_modules_still_share_one_total_read_budget(tmp_path, monkeypatch):
    for name in ['first.py', 'second.py']:
        (tmp_path / name).write_text('#' + 'x' * 100 + '\ndef build():\n    pass\n')
    monkeypatch.setattr(discovery, 'MAX_BYTES', 200)
    result = discovery.discover(tmp_path, {'entries': {
        'first.py:build': [], 'second.py:build': []}})
    assert [e['status'] for e in result['entries']] == ['recognized', 'invalid']
    assert any(d['code'] == 'entry_unreadable' and 'budget' in d['detail'].lower()
               for d in result['diagnostics'])


def test_new_discovery_reads_the_current_source(tmp_path):
    source = tmp_path / 'recipe.py'
    source.write_text('def build():\n    pass\n')
    control = {'entries': {'recipe.py:build': []}}
    before = discovery.discover(tmp_path, control)
    source.write_text('def changed():\n    pass\n')
    after = discovery.discover(tmp_path, control)
    assert after['digest'] != before['digest']
    assert after['entries'][0]['status'] == 'invalid'
    assert after['entries'][0]['sha256'] != before['entries'][0]['sha256']
