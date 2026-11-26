"""Why each piece of pre-v3 execution code is still here, and that nothing new leans on it.

The runtime v3 migration plan (archived design record unified-runtime-v3-20261114/MIGRATION.md) asks for this: retired symbols are deleted
card by card, and "仍有真实消费者的旧内部符号必须列出保留依据". ``pre_v3_retention.json`` is that
list. It names every module of the execution paths that predate ``execute_action`` and, for
modules that hold both live and old code, every top-level symbol:

    live            the v3 runtime executes it
    compat_entry    a public entry kept by the migration contract; it must translate the
                    request into one v3 action and run no loop of its own
    history         needed to read sealed plans and results of earlier runs
    frozen          referenced by one of the seven frozen files
    separate_entry  the bundle command line, a different entry kept as it is
    old             implementation of a pre-v3 path with no product caller; to be retired
    undecided       not enough evidence either way

This test reads source only. It keeps the list true (everything named exists, recorded
importers and users are exact) and keeps old code from gaining a new consumer. It does not
decide anything: moving an entry between statuses is a reviewed edit of the JSON file.
"""
import json

import pre_v3_retention_scan as scan

LEDGER = json.loads((scan.ROOT / 'tests' / 'pre_v3_retention.json').read_text())
STATUSES = {'live', 'compat_entry', 'history', 'frozen', 'separate_entry', 'old', 'undecided', 'mixed'}
PINNED = {'old', 'undecided'}
KNOWN = scan.modules()
IMPORTERS = scan.importers(KNOWN)
TOKENS = {name: scan.name_tokens(path) for name, path in KNOWN.items()}


def _entries():
    for module, entry in LEDGER['modules'].items():
        yield module, entry


def _symbols():
    for module, entry in _entries():
        for symbol, record in entry.get('symbols', {}).items():
            yield module, symbol, record


def test_the_list_is_well_formed():
    assert LEDGER['schema'] == 'demiflow_pre_v3_retention_v1'
    for module, entry in _entries():
        assert entry['status'] in STATUSES and entry['reason'], module
        assert (entry['status'] == 'mixed') == bool(entry.get('symbols')), module
        if entry['status'] in PINNED:
            assert sorted(entry['importers']) == entry['importers'], module
    for module, symbol, record in _symbols():
        assert record['status'] in STATUSES - {'mixed'} and record['reason'], (module, symbol)
        if record['status'] == 'compat_entry':
            assert record['translates_through'], (module, symbol)


def test_everything_listed_exists():
    missing = [module for module, _ in _entries() if module not in KNOWN]
    assert not missing, missing
    for module, entry in _entries():
        defined = scan.top_level(KNOWN[module])
        absent = sorted(set(entry.get('symbols', {})) - set(defined))
        assert not absent, (module, absent)


def test_a_mixed_module_accounts_for_every_function_and_class():
    for module, entry in _entries():
        if entry['status'] != 'mixed':
            continue
        defined = {name for name, kind in scan.top_level(KNOWN[module]).items() if kind != 'constant'}
        unlisted = sorted(defined - set(entry['symbols']))
        assert not unlisted, (module, unlisted)


def test_removed_parts_stay_retired():
    for entry in LEDGER['removed']:
        module = entry['module']
        if module not in KNOWN:
            continue
        symbol = entry.get('symbol')
        assert symbol is not None, f"Retired module restored: {module}"
        assert symbol not in scan.top_level(KNOWN[module]), f"Retired symbol restored: {module}.{symbol}"


def test_old_modules_have_exactly_the_recorded_importers():
    """A new importer of old code fails here; so does a stale entry after an importer is gone."""
    wrong = {}
    for module, entry in _entries():
        if entry['status'] in PINNED:
            actual = sorted(IMPORTERS.get(module, ()))
            if actual != entry['importers']:
                wrong[module] = {'recorded': entry['importers'], 'actual': actual}
    assert not wrong, json.dumps(wrong, indent=1)


def test_old_symbols_have_exactly_the_recorded_users():
    wrong = {}
    for module, symbol, record in _symbols():
        if record['status'] in PINNED:
            # A module can only reach the symbol through the defining module, so its importers
            # are the candidates; this keeps unrelated code that happens to reuse a name out.
            actual = sorted(name for name in IMPORTERS.get(module, ()) if symbol in TOKENS[name])
            if actual != sorted(record.get('users', [])):
                wrong[module + ':' + symbol] = {'recorded': sorted(record.get('users', [])), 'actual': actual}
    assert not wrong, json.dumps(wrong, indent=1)


def test_compatibility_entries_translate_to_the_v3_action():
    """The migration contract keeps these entries and removes their own launch, scheduling
    and commit loops. Each must still hand the request to the named v3 call."""
    for module, symbol, record in _symbols():
        if record['status'] != 'compat_entry':
            continue
        source = scan.function_source(KNOWN[module], symbol)
        assert source, (module, symbol)
        assert record['translates_through'] in source, (module, symbol, record['translates_through'])
        for name in record.get('must_not_call', []):
            assert name not in source, (module, symbol, name)
