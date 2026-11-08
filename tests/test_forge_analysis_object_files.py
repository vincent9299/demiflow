"""Actual owned-file atomic preparation; no DB-ready/volume/isolation claim."""
import hashlib
import json
from pathlib import Path

import pytest

from demiflow.workbench.analysis import object_files as o
from demiflow.forge.schema import ForgeError


def arguments():
    data = {"_versions/1.manifest": b"fixed version", "data/one.lance": b"bounded data"}
    return dict(publish_id="publish-one", seed=dict(project="w"+"a"*32, object_id="o"+"b"*32,
        generation=1, retention="ephemeral", execution_id="execute-one", parents=[], source_binding=None),
        description=dict(format="lance", version=1, rows=2,
            schema=[dict(name="value", type="int64", nullable=True)], schema_sha256="c"*64, logical_bytes=17, complete=True),
        files={name:dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()) for name,raw in data.items()}), data


def prepared(tmp_path):
    kwargs, data = arguments(); receiver = o.Ingress(tmp_path/"state", **kwargs)
    for name, raw in data.items():
        receiver.start_file(name); receiver.write(raw); receiver.finish_file()
    return receiver, receiver.seal()


def test_real_atomic_no_replace_keeps_unknown_existing_target_and_requires_db_authorization(tmp_path):
    receiver, result = prepared(tmp_path)
    assert result["prepared"] and not result["database_ready"]
    with pytest.raises(ForgeError):
        receiver.rename(authorize=lambda *_: False)
    target = receiver.objects_root/receiver.manifest["object_id"]
    target.mkdir(); (target/"unrelated").write_text("keep")
    with pytest.raises(ForgeError) as error:
        receiver.rename(authorize=lambda *_: True)
    assert error.value.code == "conflict" and (target/"unrelated").read_text() == "keep"
    assert receiver.stage.exists()


def test_sealed_files_can_rename_but_do_not_claim_database_ready(tmp_path):
    receiver, receipt = prepared(tmp_path)
    renamed = receiver.rename(authorize=lambda identifier,digest: identifier == receipt["publish_id"] and digest == receipt["manifest_digest"])
    assert renamed["renamed"] and renamed["database_ready"] is False and not receiver.stage.exists()
    root = receiver.objects_root/receiver.manifest["object_id"]
    assert json.loads((root/"manifest.json").read_text()) == receiver.manifest
    assert (root/"data.lance/data/one.lance").read_bytes() == b"bounded data"
    assert (root/"data.lance/data/one.lance").stat().st_mode & 0o777 == 0o400


def test_bad_hash_or_overgrowth_never_creates_visible_object(tmp_path):
    kwargs, data = arguments(); receiver = o.Ingress(tmp_path/"state", **kwargs)
    receiver.start_file("data/one.lance")
    receiver.write(b"x"*len(data["data/one.lance"]))
    with pytest.raises(ForgeError, match="content differs"):
        receiver.finish_file()
    with pytest.raises(ForgeError, match="incomplete"):
        receiver.seal()
    receiver.close()
    assert not list(receiver.objects_root.iterdir()) and receiver.stage.exists()
    second = tmp_path/"other"
    receiver = o.Ingress(second, **kwargs)
    receiver.start_file("data/one.lance")
    with pytest.raises(ForgeError) as error:
        receiver.write(b"x"*(len(data["data/one.lance"])+1))
    assert error.value.code == "resource_limit"
    receiver.close()
    assert (receiver.data/"data/one.lance").stat().st_size == 0


def test_manifest_escape_and_byte_limit_refuse_before_creating_staging(tmp_path, monkeypatch):
    kwargs, data = arguments(); kwargs["files"]["../../outside"] = kwargs["files"]["data/one.lance"]
    with pytest.raises(ForgeError):
        o.Ingress(tmp_path/"state", **kwargs)
    assert not (tmp_path/"state").exists()
    kwargs, _ = arguments(); monkeypatch.setattr(o, "MAX_ENCODED_BYTES", 4)
    with pytest.raises(ForgeError):
        o.Ingress(tmp_path/"state", **kwargs)
    assert not (tmp_path/"state").exists()


def test_generic_notebook_document_uses_same_ingress_and_keeps_partial_status(tmp_path):
    kwargs, _ = arguments()
    raw=b'{"outputs":[],"complete":false}'
    sha=hashlib.sha256(raw).hexdigest()
    kwargs["description"] = dict(format="document", mime="application/vnd.demiflow.output+json", bytes=len(raw),sha256=sha,complete=False)
    kwargs["files"] = {"document.json":dict(bytes=len(raw),sha256=sha)}
    receiver=o.Ingress(tmp_path/"state",**kwargs)
    receiver.start_file("document.json"); receiver.write(raw); receiver.finish_file()
    proof=receiver.seal(); assert not proof["database_ready"]
    receiver.rename(authorize=lambda *_:True)
    target=receiver.objects_root/kwargs["seed"]["object_id"]
    assert (target/"payload/document.json").read_bytes()==raw
    assert receiver.manifest["payload"]["complete"] is False
    assert not (target/"data.lance").exists()
