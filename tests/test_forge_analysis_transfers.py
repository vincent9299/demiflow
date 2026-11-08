"""Opaque upload framing/hash/seal on ordinary files, no Arrow/Lance/OCI decode."""
import hashlib
import os
import struct

from test_forge_analysis_records import controller


def config(raw):
    framed = struct.pack("!I", len(raw)) + raw
    return dict(operation="materialize", input_id="i"+"a"*32, stream_sha256=hashlib.sha256(framed).hexdigest(),
        schema_sha256="b"*64, encoded_bytes=len(framed), chunks=1, publish_id="publish")


def test_input_bytes_commitment_then_readonly_seal_and_wal_guard_restore(tmp_path):
    raw = b"opaque bytes; decoder stays in native job"
    cfg = config(raw)
    with controller(tmp_path/"db") as client:
        assert client.call("upload_create", key="one", config=cfg)["result"] == {"created": True}
        assert client.call("upload_consume", key="one", kind="arrow", value=raw.decode())["result"] is None
        proof = client.call("upload_consume", key="one", kind="end", value=None)["result"]
        assert proof["sealed"] and proof["stream_sha256"] == cfg["stream_sha256"]
        assert client.call("object_guard")["result"] == {"sql_rejected": True}
    path = tmp_path/"transfer/ingress"/cfg["input_id"]/"data.bin"
    assert path.read_bytes() == struct.pack("!I", len(raw)) + raw and path.stat().st_mode & 0o777 == 0o400


def test_changed_upload_or_revocation_never_seals_and_retains_partial_bytes(tmp_path):
    with controller(tmp_path/"db") as client:
        client.call("upload_create", key="one", config=config(b"abc"))
        client.call("upload_consume", key="one", kind="arrow", value="abd")
        assert client.call("upload_consume", key="one", kind="end", value=None)["code"] == "conflict"
    path = tmp_path/"transfer/ingress"/("i"+"a"*32)/"data.bin"
    assert path.exists() and path.stat().st_mode & 0o777 == 0o600
    with controller(tmp_path/"other/db") as client:
        client.call("upload_create", key="one", config=config(b"abc"))
        assert client.call("upload_consume", key="one", kind="arrow", value="abc", controls={"permissions": ["read"]})["code"] == "operation_rejected"


def exported(tmp_path, chunks):
    cfg=config(b"placeholder")
    framed=b"".join(struct.pack("!I",len(raw))+raw for raw in chunks)
    cfg.update(encoded_bytes=len(framed),chunks=len(chunks),stream_sha256=hashlib.sha256(framed).hexdigest())
    root=tmp_path/"outbox";directory=root/cfg["input_id"];directory.mkdir(parents=True)
    path=directory/"data.bin";path.write_bytes(framed);path.chmod(0o440)
    return root,path,cfg,framed


def test_export_copy_advances_one_opaque_frame_per_turn_and_checks_full_digest(tmp_path):
    root,path,cfg,framed=exported(tmp_path,[b"opaque one",b"opaque two"])
    with controller(tmp_path/"db") as client:
        assert client.call("copy_create",key="one",outbox_root=str(root),config=cfg)["result"]==dict(created=True)
        assert client.call("copy_step",key="one")["result"]==dict(complete=False,chunks=1)
        assert client.call("copy_step",key="one")["result"]==dict(complete=False,chunks=2)
        assert client.call("copy_step",key="one")["result"]==dict(complete=True,chunks=2)
        assert client.call("object_guard")["result"]==dict(sql_rejected=True)
        assert client.call("copy_close",key="one")["result"]==dict(closed=True)
    copied=tmp_path/"transfer/ingress"/cfg["input_id"]/"data.bin"
    assert copied.read_bytes()==framed and copied.stat().st_mode & 0o777==0o400


def test_changed_export_or_oversized_prefix_never_seals_or_materializes(tmp_path):
    root,path,cfg,_=exported(tmp_path,[b"one",b"two"])
    with controller(tmp_path/"db") as client:
        client.call("copy_create",key="one",outbox_root=str(root),config=cfg)
        client.call("copy_step",key="one")
        path.chmod(0o640)
        with path.open("ab") as stream:stream.write(b"changed")
        path.chmod(0o440)
        client.call("copy_step",key="one")
        assert client.call("copy_step",key="one")["code"]=="conflict"
    assert (tmp_path/"transfer/ingress"/cfg["input_id"]/"data.bin").stat().st_mode & 0o777==0o600
    other=tmp_path/"other";other.mkdir()
    root,path,cfg,_=exported(other,[b"abc"])
    path.chmod(0o640);path.write_bytes(struct.pack("!I",1024*1024+1)+b"abc");path.chmod(0o440)
    with controller(other/"db") as client:
        client.call("copy_create",key="one",outbox_root=str(root),config=cfg)
        assert client.call("copy_step",key="one")["code"]=="resource_limit"
        assert client.call("copy_close",key="one")["result"]==dict(closed=True)
    assert (other/"transfer/ingress"/cfg["input_id"]/"data.bin").stat().st_size==0
