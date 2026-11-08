"""Real outbox/unit-lock IO with a fake source encoder; not volume/OCI proof."""
import hashlib
import json
import os

import pytest

from demiflow.workbench.analysis import outbox
from demiflow.forge.schema import ForgeError

PROJECT="w"+"1"*32
SOURCE="a"*64


def box(tmp_path,monkeypatch):
    root=tmp_path/"outbox";root.mkdir();root.chmod(0o2770)
    calls=[]
    def operation(name,workspace,*,source_locator,source_snapshot,output_fd,_pass_fds):
        assert name=="analysis_export" and output_fd in _pass_fds and len(_pass_fds)==2
        calls.append(source_snapshot)
        os.write(output_fd,b"opaque test export")
        return dict(source_snapshot=SOURCE,schema_sha256="b"*64,stream_sha256=hashlib.sha256(b"opaque test export").hexdigest(),
                    encoded_bytes=len(b"opaque test export"),chunks=1,rows=1)
    monkeypatch.setattr(outbox,"operation",operation)
    return outbox.Outbox(root),calls


def export(box,identifier,authorize=lambda:True):
    return box.export(box.root,project=PROJECT,export_id=identifier,source_snapshot=SOURCE,
        source_locator=dict(kind="workspace_table",alias="table",binding="c"*64,columns=["value"]),authorize=authorize)


def test_source_export_replay_readonly_bytes_actual_unit_fence_and_cleanup(tmp_path,monkeypatch):
    value,calls=box(tmp_path,monkeypatch);identifier="i"+"a"*32
    result=export(value,identifier)
    assert export(value,identifier)==result and len(calls)==1
    path=value.root/identifier/"data.bin"
    assert path.read_bytes()==b"opaque test export" and path.stat().st_mode & 0o777==0o440
    with pytest.raises(ForgeError,match="authorized"):
        export(value,identifier,lambda:False)
    unit=os.open(value.root/identifier/"unit.lock",os.O_RDWR)
    import fcntl
    fcntl.flock(unit,fcntl.LOCK_SH)
    try:
        with pytest.raises(ForgeError,match="not been proved"):
            value.release(project=PROJECT,export_id=identifier,source_snapshot=SOURCE)
        assert path.exists()
    finally:os.close(unit)
    assert value.release(project=PROJECT,export_id=identifier,source_snapshot=SOURCE)=={"released":True}
    assert not path.exists()


def test_incomplete_export_is_not_rerun_and_count_quota_precedes_new_file_or_process(tmp_path,monkeypatch):
    value,calls=box(tmp_path,monkeypatch)
    for suffix in ("a","b","c"):export(value,"i"+suffix*32)
    with pytest.raises(ForgeError,match="full"):
        export(value,"i"+"d"*32)
    assert len(calls)==3 and len(list(value.root.iterdir()))==3 and not (value.root/("i"+"d"*32)).exists()
    identifier="i"+"a"*32
    path=value.root/identifier/"manifest.json";record=json.loads(path.read_bytes());record.update(state="pending",result=None)
    path.chmod(0o640);path.write_text(json.dumps(record));path.chmod(0o640)
    with pytest.raises(ForgeError,match="no automatic replay"):
        export(value,identifier)
    assert len(calls)==3
    (value.root/identifier/"unknown.txt").write_text("owned admin edit must be preserved")
    with pytest.raises(ForgeError,match="unknown"):
        value.release(project=PROJECT,export_id=identifier,source_snapshot=SOURCE)
    assert (value.root/identifier/"unknown.txt").exists()


def test_helper_cleanup_requires_original_unit_scope_and_no_unknown_files(tmp_path,monkeypatch):
    # Same-UID fixture tests the real filesystem/unit fence; distinct host/Web
    # UID and shared-group mappings still require the admitted B1 environment.
    value,_=box(tmp_path,monkeypatch);identifier="i"+"a"*32
    export(value,identifier)
    def release(**extra):
        return outbox.release_from_helper(value.root,project=PROJECT,export_id=identifier,source_snapshot=SOURCE,
            allowed_web_uids=[os.geteuid()],helper_gid=os.getegid(),witness=extra.get("witness",lambda:True))
    with pytest.raises(ForgeError,match="active IO"):
        release(witness=lambda:False)
    import fcntl
    unit=os.open(value.root/identifier/"unit.lock",os.O_RDWR)
    fcntl.flock(unit,fcntl.LOCK_SH)
    try:
        with pytest.raises(ForgeError,match="active IO"):release()
    finally:os.close(unit)
    unknown=value.root/identifier/"extra";unknown.write_text("preserve")
    with pytest.raises(ForgeError,match="Unknown"):release()
    assert unknown.read_text()=="preserve"
    unknown.unlink()
    assert release() is True and not (value.root/identifier).exists()
    assert release() is True  # missing original directory, no latest fallback
