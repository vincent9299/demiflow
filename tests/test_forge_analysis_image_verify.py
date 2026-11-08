"""Image file substitution/extra file/mode refusal without executing image code."""
import hashlib
import json
import os

import pytest

from demiflow.workbench.analysis.image_verify import verify_rootfs
from demiflow.forge.schema import ForgeError


def fixture(tmp_path):
    root = tmp_path/"rootfs"; root.mkdir()
    source = root/"worker.py"; source.write_bytes(b"raise RuntimeError('never execute image contents')\n"); source.chmod(0o444)
    root.chmod(0o555)
    runtime = {"worker.py": dict(sha256=hashlib.sha256(source.read_bytes()).hexdigest(), bytes=source.stat().st_size, mode=0o444)}
    lock = dict(format="demiflow_analysis_image_lock_v1", files_count=1, bytes=source.stat().st_size,
        image_tree_sha256=hashlib.sha256(json.dumps(runtime, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        files={"worker.py": dict(**runtime["worker.py"], source=None, provenance="test")})
    return root, source, lock


def test_full_file_hash_then_mutated_content_refused(tmp_path):
    root, source, lock = fixture(tmp_path)
    result = verify_rootfs(root, lock, allowed_owners={os.geteuid()})
    assert result["files"] == 1 and result["image_executed"] is False and result["isolation_verified"] is False
    source.chmod(0o644); source.write_bytes(b"x"*source.stat().st_size); source.chmod(0o444)
    with pytest.raises(ForgeError, match="content"):
        verify_rootfs(root, lock, allowed_owners={os.geteuid()})
    root.chmod(0o700)


def test_unknown_files_links_and_writable_modes_refused(tmp_path):
    root, source, lock = fixture(tmp_path)
    source.chmod(0o644)
    with pytest.raises(ForgeError, match="mode"):
        verify_rootfs(root, lock, allowed_owners={os.geteuid()})
    source.chmod(0o444); root.chmod(0o755)
    extra = root/"unknown"; extra.write_bytes(b"unreviewed"); extra.chmod(0o444); root.chmod(0o555)
    with pytest.raises(ForgeError, match="unreviewed"):
        verify_rootfs(root, lock, allowed_owners={os.geteuid()})
    root.chmod(0o755); extra.unlink(); extra.symlink_to(source); root.chmod(0o555)
    with pytest.raises(ForgeError, match="symlink"):
        verify_rootfs(root, lock, allowed_owners={os.geteuid()})
    root.chmod(0o700)
