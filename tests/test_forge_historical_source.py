"""A historical source receipt keeps its paths when the installed package moves."""
import hashlib
import json

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import legacy_wav_recovery as recovery


def test_source33_authorization_reads_original_source_identity_only(tmp_path, monkeypatch):
    # A private synthetic snapshot, never the real historical recovery target.
    source = tmp_path / 'source33'
    files = {}
    for name in recovery.CLOSURE:
        relative = 'demiflow/forge/analysis/' + name
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('# saved Source33 source: ' + name + '\n')
        files[relative] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

    def save(name, value):
        path = tmp_path / name
        path.write_text(json.dumps(value))
        path.chmod(0o600)
        return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

    manifest = save('source-manifest.json', {'files': files})
    monkeypatch.setattr(recovery, 'SOURCE33', manifest['sha256'])
    proof = save('proof.json', {})
    authorization = dict(
        format=recovery.AUTH, state=str(tmp_path / 'state'), job_id='private-job',
        read_request_id='private-read', lease_sha256='0' * 64,
        context_sha256='0' * 64, owner_sha256='0' * 64, origin_digest='0' * 64,
        unit_identity={}, artifact={}, source_root=str(source), source_manifest=manifest,
        installation_receipt=proof, review=proof, maintenance_allowed=True,
        proof_scope=recovery.SCOPE, controller_lock_identity={}, filesystem_test=proof,
        original_lease_snapshot=proof, cleanup_sha256=None,
    )
    auth = save('authorization.json', authorization)
    before = {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    value, closure = recovery.authorization(auth['path'], auth['sha256'])
    assert value == authorization
    assert closure == {path: item['sha256'] for path, item in files.items()}
    assert {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before
    assert not (source / 'demiflow/workbench').exists()

    # A matching copy under the current package path cannot substitute for a
    # changed historical source named by the original manifest.
    name = recovery.CLOSURE[0]
    old = source / 'demiflow/forge/analysis' / name
    current = source / 'demiflow/workbench/analysis' / name
    current.parent.mkdir(parents=True)
    current.write_bytes(old.read_bytes())
    old.write_text('# changed after original review\n')
    with pytest.raises(ForgeError, match='Original Source33 recovery closure changed') as caught:
        recovery.authorization(auth['path'], auth['sha256'])
    assert caught.value.code == 'conflict'
