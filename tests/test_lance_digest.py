"""rowhash-v1 / rowhash-v1-agg 契约测试（不依赖 lance）。

黄金向量锁定已发布编码行为：任何编码规则变更必须升 rowhash-v2，
不得原地改语义（digest.py 模块 docstring 的算法演进约束）。
"""
import pytest

from demiflow.lance.digest import (
    ROWHASH_V1, TABLE_DIGEST_V1, aggregate_digest_v1, rowhash_v1,
    table_digest_v1,
)
from demiflow.lance.refs import DatasetRef

import re

_DIGEST_RE = re.compile(r"^rowhash-v1:[0-9a-f]{64}$")
_AGG_RE = re.compile(r"^rowhash-v1-agg:[0-9a-f]{64}$")


def test_constants_and_prefix_format():
    assert ROWHASH_V1 == "rowhash-v1"
    assert TABLE_DIGEST_V1 == "rowhash-v1-agg"
    assert _DIGEST_RE.match(rowhash_v1({"a": 1}))
    assert _AGG_RE.match(table_digest_v1([{"a": 1}]))


def test_deterministic_and_key_order_independent():
    row1 = {"b": 2, "a": 1, "nested": {"y": None, "x": [1, "2"]}}
    row2 = {"a": 1, "b": 2, "nested": {"x": [1, "2"], "y": None}}
    assert rowhash_v1(row1) == rowhash_v1(row2)
    assert rowhash_v1(row1) == rowhash_v1(row1)


def test_type_tags_distinguish_values():
    values = [None, True, False, 0, 1, 1.0, "1", b"1", [1], {"1": 1}]
    digests = {rowhash_v1({"v": v}) for v in values}
    assert len(digests) == len(values)


def test_length_prefix_disambiguates_concatenation():
    # 无长度前缀时 [["a","b"]] 与 [["ab"]] 的展平编码会相同
    assert rowhash_v1({"v": ["a", "b"]}) != rowhash_v1({"v": ["ab"]})
    assert rowhash_v1({"a": "bc"}) != rowhash_v1({"ab": "c"})
    assert rowhash_v1({"v": ["", "a"]}) != rowhash_v1({"v": ["a", ""]})


def test_bytes_and_unicode():
    assert rowhash_v1({"v": b"abc"}) != rowhash_v1({"v": "abc"})
    assert rowhash_v1({"v": "中文"}) == rowhash_v1({"v": "中文"})
    assert rowhash_v1({"v": "中文"}) != rowhash_v1({"v": "中文".encode("utf-8")})
    assert rowhash_v1({"v": bytearray(b"x")}) == rowhash_v1({"v": b"x"})


def test_non_finite_floats_rejected():
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            rowhash_v1({"v": bad})


def test_non_str_dict_keys_rejected():
    with pytest.raises(TypeError):
        rowhash_v1({1: "a"})


def test_unsupported_types_rejected():
    class Custom:
        pass

    with pytest.raises(TypeError):
        rowhash_v1({"v": Custom()})


def test_golden_vector_rowhash_v1():
    # 编码快照：d:1:s:1:v s:5:hello -> sha256。若此断言失败说明编码被改动，
    # 必须升版本而不是静默修改。
    golden = rowhash_v1({"v": "hello"})
    assert golden == "rowhash-v1:" + _sha256(b"d:1:s:1:vs:5:hello")


def test_golden_vector_scalar_encodings():
    assert rowhash_v1(None) == "rowhash-v1:" + _sha256(b"n:")
    assert rowhash_v1([]) == "rowhash-v1:" + _sha256(b"l:0:")
    assert rowhash_v1({}) == "rowhash-v1:" + _sha256(b"d:0:")
    assert rowhash_v1(1.5) == "rowhash-v1:" + _sha256(b"f:1.5")
    assert rowhash_v1(True) == "rowhash-v1:" + _sha256(b"b:1")
    assert rowhash_v1(0) == "rowhash-v1:" + _sha256(b"i:0")


def test_table_digest_order_and_count_sensitive():
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    assert table_digest_v1(rows) == table_digest_v1(list(rows))
    assert table_digest_v1(rows) != table_digest_v1(rows[::-1])
    # 末尾行数入摘要：截断／追加空行摘要不可伪造同值
    assert table_digest_v1(rows) != table_digest_v1(rows[:2])
    digests = [rowhash_v1(r) for r in rows]
    assert aggregate_digest_v1(digests) == table_digest_v1(rows)
    # 带算法前缀与裸 hex 两种输入等价
    assert aggregate_digest_v1(d.split(":", 1)[1] for d in digests) == table_digest_v1(rows)


def test_table_digest_empty_table_valid():
    # 零行提交是有效空表，其摘要也应稳定可算
    assert _AGG_RE.match(table_digest_v1([]))
    assert table_digest_v1([]) == aggregate_digest_v1([])


def test_aggregate_rejects_malformed_row_digest():
    for bad in ("abc", "x" * 64, "rowhash-v1:" + "g" * 64):
        with pytest.raises(ValueError):
            aggregate_digest_v1([bad])


def test_dataset_ref_accepts_table_digest():
    ref = DatasetRef(
        dataset_id="telemetry/sensors/v1/readings",
        relative_uri="telemetry/sensors/v1/readings.lance",
        lance_version=1,
        schema_name="sensor_readings",
        schema_version="v1",
        schema_hash="sha256:" + "0" * 64,
        row_count=2,
        content_digest=table_digest_v1([{"a": 1}, {"a": 2}]),
    )
    assert ref.content_digest.startswith("rowhash-v1-agg:")
    assert DatasetRef.from_dict(ref.to_dict()) == ref


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()
