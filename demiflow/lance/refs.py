"""DatasetRef：表版本的稳定引用（通用平台机制）。

引用只保存相对位置（store_id + relative_uri）与具体 Lance 版本；解析为
绝对 URI 依赖调用方传入的数据根，因此搬迁数据根不改变身份。引用一经
创建即不可变，运行中不得追随 latest。

字段与序列化格式（to_dict/from_dict）是登记表在册数据的既有契约，
不得增删字段或改变校验语义；content_digest 算法须自带版本前缀
（见 demiflow.lance.digest）。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-zA-Z._:-]{1,128}$")


def _clean_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{label} must be a normalized non-empty string")
    return value


@dataclass(frozen=True)
class DatasetRef:
    """一个具体表版本的完整引用。

    content_digest 是规范化内容摘要（算法须自带版本前缀，如
    ``rowhash-v1:...``）；尚未计算时为 None，不能以行数或文件哈希冒充。
    """

    dataset_id: str
    relative_uri: str
    lance_version: int
    schema_name: str
    schema_version: str
    schema_hash: str
    row_count: int
    store_id: str = "local"
    content_digest: str | None = None

    def __post_init__(self) -> None:
        _clean_text(self.dataset_id, "dataset_id")
        _clean_text(self.relative_uri, "relative_uri")
        if self.relative_uri.startswith(("/", "\\")) or ".." in self.relative_uri.split("/"):
            raise ValueError("relative_uri must be a safe relative path")
        _clean_text(self.schema_name, "schema_name")
        _clean_text(self.schema_version, "schema_version")
        _clean_text(self.schema_hash, "schema_hash")
        _clean_text(self.store_id, "store_id")
        if (
            isinstance(self.lance_version, bool)
            or not isinstance(self.lance_version, int)
            or self.lance_version <= 0
        ):
            raise ValueError("lance_version must be a positive integer")
        if (
            isinstance(self.row_count, bool)
            or not isinstance(self.row_count, int)
            or self.row_count < 0
        ):
            raise ValueError("row_count must be a non-negative integer")
        if self.content_digest is not None:
            _clean_text(self.content_digest, "content_digest")
            algorithm, separator, value = self.content_digest.partition(":")
            if (
                not separator
                or not _DIGEST_PATTERN.match(f"{algorithm}:x")
                or not value
                or value != value.strip()
                or " " in value
            ):
                raise ValueError(
                    "content_digest must carry an algorithm version prefix "
                    "(for example rowhash-v1:<hex>)",
                )
        if not _SHA256_PATTERN.match(self.schema_hash) and ":" not in self.schema_hash:
            # schema_hash 形如 "sha256:<hex>"（demiflow.lance.storage.schema_hash）；
            # 自定义前缀也要求带冒号
            raise ValueError("schema_hash must be a versioned digest string")

    @property
    def identity(self) -> tuple[str, str, int]:
        """登记主键：dataset_id + 相对位置 + Lance 版本。"""
        return (self.dataset_id, self.relative_uri, self.lance_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "store_id": self.store_id,
            "relative_uri": self.relative_uri,
            "lance_version": self.lance_version,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "schema_hash": self.schema_hash,
            "row_count": self.row_count,
            "content_digest": self.content_digest,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DatasetRef":
        fields = {
            "dataset_id", "store_id", "relative_uri", "lance_version",
            "schema_name", "schema_version", "schema_hash", "row_count",
            "content_digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("DatasetRef fields are unsupported")
        return cls(**dict(value))

    def resolve(self, root) -> str:
        """解析为绝对表 URI；root 是调用方的数据根（Path 或 str）。

        数据根位置由调用方配置；引用本身只保存相对位置，搬迁根不改身份。
        """
        return str((Path(root) / self.relative_uri).resolve(strict=False))

    def open(self, root, storage_options=None):
        """按固定版本打开 Lance Dataset；不追随 latest。"""
        from .storage import normalize_storage_options, open_lance_dataset, schema_hash

        options = normalize_storage_options(storage_options)
        dataset = open_lance_dataset(self.resolve(root), self.lance_version, options)
        if dataset.count_rows() != self.row_count:
            raise ValueError('DatasetRef row_count differs from the pinned table version')
        if schema_hash(dataset.schema) != self.schema_hash:
            raise ValueError('DatasetRef schema_hash differs from the pinned table version')
        return dataset


__all__ = ["DatasetRef"]
