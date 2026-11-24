"""The original write seam's published plan pin and source archive remain intact."""
import hashlib
import json

from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.journal.run_journal import RunJournal
from test_u2_chain_commit import original_chain


def test_original_write_seam_program_and_archive_survive_without_the_writer(original_chain):
    root, original = original_chain
    ref = original['write_program']
    assert set(ref) == {'path', 'sha256', 'bytes', 'code_pin_digest', 'code_sources'}
    path = root / ref['path']
    raw = path.read_bytes()
    assert len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha256']
    pin = json.loads(raw)
    assert identity(pin) == ref['code_pin_digest']
    journal = RunJournal(root)
    with journal._connection() as db:
        pins = [json.loads(row[0]) for row in db.execute('SELECT document FROM runtime_sources')]
    assert pin in pins
    entry, = [item for item in ref['code_sources'] if item['source'].endswith('/chain_business.py')]
    archived = (root / entry['path']).read_bytes()
    assert len(archived) == entry['bytes']
    assert hashlib.sha256(archived).hexdigest() == entry['sha256'] == pin['dependencies']['chain_business']['sha256']
    assert (root.parent / 'chain_business.py').read_bytes() == archived
