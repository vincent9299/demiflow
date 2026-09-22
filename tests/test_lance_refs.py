"""DatasetRef 契约测试（不依赖 lance）：校验、序列化、解析与身份。"""
import pytest

from demiflow.lance.refs import DatasetRef


def make_ref(**overrides):
    fields = dict(
        dataset_id="telemetry/room/materials",
        relative_uri="telemetry/room/r1/materials.lance",
        lance_version=3,
        schema_name="sensor_readings",
        schema_version="v1",
        schema_hash="sha256:" + "0" * 64,
        row_count=12,
    )
    fields.update(overrides)
    return DatasetRef(**fields)


def test_roundtrip_strict():
    ref = make_ref(content_digest="rowhash-v1:" + "a" * 16)
    clone = DatasetRef.from_dict(ref.to_dict())
    assert clone == ref
    bad = ref.to_dict()
    bad.pop("row_count")
    with pytest.raises(ValueError):
        DatasetRef.from_dict(bad)


def test_rejects_bad_values():
    with pytest.raises(ValueError):
        make_ref(lance_version=0)
    with pytest.raises(ValueError):
        make_ref(row_count=-1)
    with pytest.raises(ValueError):
        make_ref(relative_uri="/absolute/not/allowed")
    with pytest.raises(ValueError):
        make_ref(relative_uri="telemetry/../etc/passwd")
    with pytest.raises(ValueError):
        make_ref(content_digest="no-algorithm-prefix")
    with pytest.raises(ValueError):
        make_ref(dataset_id=" padded ")


def test_resolve_uses_caller_root(tmp_path):
    ref = make_ref()
    assert ref.resolve(tmp_path) == str(tmp_path / ref.relative_uri)
    # 相对位置：换根不改身份
    assert ref.resolve(tmp_path / "moved") == str(
        tmp_path / "moved" / ref.relative_uri)


def test_identity_key_ignores_content_fields():
    a = make_ref()
    b = make_ref(row_count=99)
    assert a.identity == b.identity  # content fields are not part of identity
