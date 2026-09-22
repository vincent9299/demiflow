"""行级内容摘要统一算法（rowhash-v1）与表级聚合（rowhash-v1-agg）。

DatasetRef.content_digest 要求算法自带版本前缀；本模块定义两个算法：

- ``rowhash-v1``：单行业务内容摘要。与存储顺序、schema 字段顺序、
  dict 插入顺序无关；同内容必同摘要、异内容（含类型差异）必异摘要。
- ``rowhash-v1-agg``：表级摘要（DatasetRef.content_digest 用）。按表内
  行顺序聚合行摘要，顺序与行数都影响结果；零行表有效。

规范编码（canonical encoding）
------------------------------
行先编码为确定性字节串再取 sha256。类型逐个带标签，长度前缀杜绝
拼接歧义；不依赖 JSON 的浮点／bytes 表达差异。具体规则：

- None            -> ``n:``
- True/False      -> ``b:1`` / ``b:0``
- int             -> ``i:`` + 十进制（仅 ``0`` 无前导零，负数带 ``-``）
- float（有限）    -> ``f:`` + 最短往返十进制 repr（Python 3 repr；
                     其他语言须用等价的最短往返格式，如 Ryu/Grisu）
- str             -> ``s:`` + UTF-8 字节数 + ``:`` + UTF-8 字节
- bytes/bytearray -> ``y:`` + 字节数 + ``:`` + 原始字节
- list/tuple      -> ``l:`` + 元素个数 + ``:`` + 各元素编码依次拼接
- dict            -> ``d:`` + 键个数 + ``:`` + 按 key 的 UTF-8 字节序
                     升序，每键输出 键编码+值编码

约束：dict 键必须是 str；float 必须有限（NaN/inf 报错）。str 排序按
UTF-8 字节序，与 Unicode 码点序一致，跨语言可实现。

摘要格式：``rowhash-v1:<64 位小写 hex>``。

表级聚合
--------
输入为行摘要 hex 序列（每行 64 位小写 hex），依次对
``rowhash-v1-agg\\n`` + 各行 hex 拼接 + ``\\n<行数>`` 的 ASCII 字节取
sha256，输出 ``rowhash-v1-agg:<64 位小写 hex>``。行数入摘要使截断
不可伪造；固定宽 hex 拼接无歧义；聚合为流式，行数不限。

算法演进：任何编码规则变更必须升版本号（rowhash-v2），不得原地改语义；
本模块的黄金向量测试锁定已发布行为。
"""
from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Iterator, Mapping

ROWHASH_V1 = "rowhash-v1"
TABLE_DIGEST_V1 = "rowhash-v1-agg"
_HEX = "0123456789abcdef"


def _encode(value, out: list) -> None:
    if value is None:
        out.append(b"n:")
    elif isinstance(value, bool):
        out.append(b"b:1" if value else b"b:0")
    elif isinstance(value, int):
        out.append(b"i:" + str(value).encode("ascii"))
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("rowhash-v1 cannot digest non-finite floats")
        out.append(b"f:" + repr(value).encode("ascii"))
    elif isinstance(value, str):
        data = value.encode("utf-8")
        out.append(b"s:" + str(len(data)).encode("ascii") + b":" + data)
    elif isinstance(value, (bytes, bytearray)):
        out.append(b"y:" + str(len(value)).encode("ascii") + b":" + bytes(value))
    elif isinstance(value, (list, tuple)):
        out.append(b"l:" + str(len(value)).encode("ascii") + b":")
        for item in value:
            _encode(item, out)
    elif isinstance(value, Mapping):
        items = list(value.items())
        try:
            items.sort(key=lambda kv: kv[0].encode("utf-8"))
        except AttributeError:
            raise TypeError("rowhash-v1 requires str dict keys") from None
        out.append(b"d:" + str(len(items)).encode("ascii") + b":")
        for key, val in items:
            out.append(b"s:" + str(len(key.encode("utf-8"))).encode("ascii")
                       + b":" + key.encode("utf-8"))
            _encode(val, out)
    else:
        raise TypeError(f"rowhash-v1 cannot digest {type(value).__name__}")


def rowhash_v1(row: Mapping) -> str:
    """单行业务内容摘要，形如 ``rowhash-v1:<64hex>``。"""
    chunks: list[bytes] = []
    _encode(row, chunks)
    return f"{ROWHASH_V1}:{hashlib.sha256(b''.join(chunks)).hexdigest()}"


def aggregate_digest_v1(row_digests: Iterable[str]) -> str:
    """表级摘要：按顺序聚合行摘要（各为 ``rowhash-v1:<64hex>`` 或裸 hex）。"""
    count = 0
    hasher = hashlib.sha256()
    hasher.update(f"{TABLE_DIGEST_V1}\n".encode("ascii"))
    for digest in row_digests:
        hex_value = digest.split(":", 1)[1] if ":" in digest else digest
        if len(hex_value) != 64 or any(c not in _HEX for c in hex_value):
            raise ValueError(f"malformed row digest: {digest!r}")
        hasher.update(hex_value.encode("ascii"))
        count += 1
    hasher.update(f"\n{count}".encode("ascii"))
    return f"{TABLE_DIGEST_V1}:{hasher.hexdigest()}"


def table_digest_v1(rows: Iterable[Mapping]) -> str:
    """对行序列先逐行 rowhash-v1 再聚合，返回表级摘要。"""
    return aggregate_digest_v1(_row_digests(rows))


def _row_digests(rows: Iterable[Mapping]) -> Iterator[str]:
    for row in rows:
        yield rowhash_v1(row)


__all__ = ["ROWHASH_V1", "TABLE_DIGEST_V1", "aggregate_digest_v1",
           "rowhash_v1", "table_digest_v1"]
