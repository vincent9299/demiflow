"""Native adapter input and refusal tests; no unisolated SQL is executed."""
import copy
import hashlib
import os
from pathlib import Path

import pytest

from demiflow.workbench.analysis import query
from demiflow.workbench.analysis import result_transfer
from demiflow.workbench.analysis import sandbox
from demiflow.forge.schema import ForgeError


def config():
    return dict(operation="query", sql="select * from orders", publish_id="p"+"a"*32,
                sources=[dict(alias="orders", object_id="o"+"b"*32)])


def test_query_binds_only_private_lance_v1_and_has_no_callback_provider_or_uri_override():
    assert query.query_config(config()) == {"orders": {"uri": "/sources/o"+"b"*32+"/data.lance", "version": 1}}
    for key, value in (("version", 2), ("uri", "/home/user/table.lance"), ("provider", "python"), ("batch_transform", "callable")):
        bad = config(); bad["sources"][0][key] = value
        with pytest.raises(ForgeError):
            query.query_config(bad)
    for value in ("../outside", "o"+"b"*32+"/file", {}, None):
        bad = config(); bad["sources"][0]["object_id"] = value
        with pytest.raises(ForgeError):
            query.query_config(bad)


def test_native_entrypoint_refuses_before_engine_call_without_real_owned_sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(sandbox, "JOB_PATH", tmp_path/"missing.json")
    monkeypatch.setattr(query, "run_query", lambda _: pytest.fail("engine must not start"))
    with pytest.raises(ForgeError, match="no host execution fallback"):
        query.main()


def test_completed_file_manifest_is_bounded_and_does_not_follow_links(tmp_path):
    root = tmp_path/"result"; (root/"_versions").mkdir(parents=True)
    (root/"_versions/1.manifest").write_bytes(b"manifest")
    (root/"part.lance").write_bytes(b"data")
    files = result_transfer.files_manifest(root)
    assert files["part.lance"] == {"bytes": 4, "sha256": hashlib.sha256(b"data").hexdigest()}
    (root/"bad").symlink_to(tmp_path/"outside")
    with pytest.raises(ForgeError, match="symlink"):
        result_transfer.files_manifest(root)
    for name in ("../x", "/tmp/x", "a/../../b", "a//b", "a/./b", "a\\b"):
        with pytest.raises(ForgeError):
            result_transfer.relative_file(name)


def test_native_file_byte_and_count_limits_are_admitted_before_hashing(tmp_path, monkeypatch):
    root = tmp_path/"result"; root.mkdir()
    (root/"one").write_bytes(b"12345")
    monkeypatch.setattr(result_transfer, "MAX_ENCODED_BYTES", 4)
    with pytest.raises(ForgeError) as error:
        result_transfer.files_manifest(root)
    assert error.value.code == "resource_limit"


@pytest.mark.parametrize('change',['append','truncate','replace','rewrite'])
def test_file_hash_is_bounded_and_refuses_mutation_or_name_replacement(tmp_path,monkeypatch,change):
    root=tmp_path/'result';root.mkdir();path=root/'part.lance';path.write_bytes(b'data')
    original=os.read;read_bytes=0;changed=False
    def read(descriptor,size):
        nonlocal read_bytes,changed
        value=original(descriptor,size);read_bytes+=len(value)
        if not changed:
            changed=True
            if change=='append':
                with path.open('ab') as stream:stream.write(b'extended')
            elif change=='truncate':path.write_bytes(b'')
            elif change=='rewrite':path.write_bytes(b'xxxx')
            else:
                replacement=tmp_path/'replacement';replacement.write_bytes(b'data');replacement.replace(path)
        return value
    monkeypatch.setattr(result_transfer.os,'read',read)
    with pytest.raises(ForgeError) as error:result_transfer.files_manifest(root)
    assert error.value.code=='conflict' and read_bytes<=5


def test_manifest_refuses_nonregular_file_without_blocking_on_open(tmp_path):
    root=tmp_path/'result';root.mkdir();os.mkfifo(root/'pipe')
    with pytest.raises(ForgeError,match='regular files'):result_transfer.files_manifest(root)
