"""数据集版本登记（catalog）、多表发布登记（releases）与登记式写入。

- ``Catalog``：长期增量登记表（数据根 registry/datasets.lance）。登记行
  以（dataset_id, relative_uri, lance_version）为主键；完全重复幂等跳过，
  同主键不同内容是冲突，必须报错而不是覆盖。显式 schema、单写者文件锁、
  批量追加、登记前查重对账。
- ``ReleaseRegistry``：发布登记（registry/releases.lance）。一次发布＝多张
  表的固定版本组合；登记是可见边界，登记前逐引用核验（catalog 在册＋固定
  版本可读＋schema/行数一致），任何不符在写入可见行之前失败。
- ``write_registered_table``：checkpoint_lance 原子提交＋回执读取＋引用
  构造＋catalog 登记的公共写端。schema 由调用方显式传入——本层不知道任何
  业务 schema 集合。

登记/发布表的磁盘格式是在册契约：字段、schema、相对位置不得变更。
读取经 lance 直读——这是存储记账，不是第二编排引擎；执行经 DataAPI。
"""
from __future__ import annotations

import fcntl
import json
import os
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from .control import control_directory, table_lock_path

from .refs import DatasetRef
from .storage import open_lance_dataset, schema_hash as lance_schema_hash

REGISTRY_RELATIVE = "registry/datasets.lance"
RELEASES_RELATIVE = "registry/releases.lance"

_REGISTRY_FIELDS = (
    "dataset_id", "store_id", "relative_uri", "lance_version",
    "schema_name", "schema_version", "schema_hash", "row_count",
    "content_digest", "registered_at",
)


class CatalogConflict(Exception):
    """同主键不同内容的登记冲突。"""


class ReleaseConflict(Exception):
    pass


def _now_micros() -> int:
    return int(time.time() * 1_000_000)


def _registry_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("store_id", pa.string(), nullable=False),
        pa.field("relative_uri", pa.string(), nullable=False),
        pa.field("lance_version", pa.int64(), nullable=False),
        pa.field("schema_name", pa.string(), nullable=False),
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("schema_hash", pa.string(), nullable=False),
        pa.field("row_count", pa.int64(), nullable=False),
        pa.field("content_digest", pa.string(), nullable=True),
        pa.field("registered_at", pa.int64(), nullable=False),
    ])


def _validate_registry_row(row: dict) -> None:
    if set(row) != set(_REGISTRY_FIELDS):
        raise ValueError("registry row fields are unsupported")


def _ref_from_row(row: Mapping) -> DatasetRef:
    return DatasetRef(
        dataset_id=row["dataset_id"],
        store_id=row["store_id"],
        relative_uri=row["relative_uri"],
        lance_version=row["lance_version"],
        schema_name=row["schema_name"],
        schema_version=row["schema_version"],
        schema_hash=row["schema_hash"],
        row_count=row["row_count"],
        content_digest=row["content_digest"],
    )


class Catalog:
    """单写者登记表。当前为本地文件系统（fcntl 锁）；远端 store 后续引入。"""

    # 并行 worker 撞登记锁的最长等待；超时说明写入方调度异常，按冲突报错。
    register_lock_wait_s = 120.0

    def __init__(self, root) -> None:
        self.root = Path(root)

    @property
    def uri(self) -> str:
        return str(self.root / REGISTRY_RELATIVE)

    # ------------------------------------------------------------------ read

    def rows(self) -> list[dict]:
        if not Path(self.uri).exists():
            return []
        import lance

        table = lance.dataset(self.uri).to_table()
        return table.to_pylist()

    def registered(self) -> list[DatasetRef]:
        return [_ref_from_row(row) for row in self.rows()]

    def get(self, dataset_id: str, *, relative_uri: str, lance_version: int) -> DatasetRef | None:
        if not Path(self.uri).exists():
            return None
        import lance
        rows = lance.dataset(self.uri).to_table(filter=(
            f"dataset_id = {_sql_string(dataset_id)} AND "
            f"relative_uri = {_sql_string(relative_uri)} AND lance_version = {int(lance_version)}"
        )).to_pylist()
        if len(rows) > 1:
            raise CatalogConflict("duplicate catalog identity")
        return _ref_from_row(rows[0]) if rows else None

    def versions(self, dataset_id: str) -> list[DatasetRef]:
        """按登记时间有序的全部版本；不依赖扫描顺序。"""
        rows = self.rows()
        matched = [
            (row.get("registered_at"), index, row)
            for index, row in enumerate(rows)
            if row["dataset_id"] == dataset_id
        ]
        matched.sort(key=lambda item: (item[0] is None, item[0], item[1]))
        return [_ref_from_row(row) for _, _, row in matched]

    def latest(self, dataset_id: str) -> DatasetRef | None:
        versions = self.versions(dataset_id)
        return versions[-1] if versions else None

    # ----------------------------------------------------------------- write

    def register(self, ref: DatasetRef) -> None:
        """登记一个版本；幂等（完全一致跳过），冲突报错。

        并行 worker 场景下锁竞争等待而非立即失败；等不到锁仍是
        CatalogConflict——并行写入方应修复调度而不是静默丢登记。
        """
        if not isinstance(ref, DatasetRef):
            raise TypeError("register expects a DatasetRef")
        import pyarrow as pa

        path = Path(self.uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = table_lock_path(path)
        with lock_path.open("a") as lock:
            deadline = time.monotonic() + self.register_lock_wait_s
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise CatalogConflict(
                            "another writer holds the catalog lock",
                        ) from exc
                    time.sleep(0.5)
            existing = self.get(ref.dataset_id, relative_uri=ref.relative_uri,
                                lance_version=ref.lance_version)
            if existing is not None:
                if existing.to_dict() == ref.to_dict():
                    return
                raise CatalogConflict(
                    f"catalog identity already registered with different "
                    f"content: {ref.dataset_id} {ref.relative_uri} "
                    f"v{ref.lance_version}",
                )
            row = dict(ref.to_dict())
            row["registered_at"] = _now_micros()
            _validate_registry_row(row)
            table = pa.Table.from_pylist([row], schema=_registry_schema())
            import lance

            lance.write_dataset(table, self.uri, mode="append")

    def export(self, target) -> Path:
        """导出当前登记快照为 JSONL（诊断／外部工具用，非权威源）。"""
        payload = "\n".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True)
            for row in self.rows()
        )
        out = Path(target)
        partial = out.with_name(out.name + "." + uuid.uuid4().hex + ".partial")
        partial.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
        os.replace(partial, out)
        return out


def _releases_schema():
    import pyarrow as pa

    return pa.schema([
        pa.field("release_id", pa.string(), nullable=False),
        pa.field("release_kind", pa.string(), nullable=False),
        pa.field("pipeline_run", pa.string(), nullable=True),
        pa.field("table_refs", pa.large_string(), nullable=False),  # JSON: [DatasetRef.to_dict()]
        pa.field("validation", pa.large_string(), nullable=True),   # JSON: 对账摘要或报告路径
        pa.field("status", pa.string(), nullable=False),            # registered
        pa.field("registered_at", pa.int64(), nullable=False),
        pa.field("previous_release_id", pa.string(), nullable=True),
    ])


class ReleaseRegistry:
    def __init__(self, root) -> None:
        self.root = Path(root)

    @property
    def uri(self) -> str:
        return str(self.root / RELEASES_RELATIVE)

    def rows(self) -> list[dict]:
        if not Path(self.uri).exists():
            return []
        import lance

        return lance.dataset(self.uri).to_table().to_pylist()

    def get(self, release_id: str) -> dict | None:
        if not Path(self.uri).exists():
            return None
        import lance
        rows = lance.dataset(self.uri).to_table(
            filter=f"release_id = {_sql_string(release_id)}").to_pylist()
        if len(rows) > 1:
            raise ReleaseConflict("duplicate release identity")
        return rows[0] if rows else None

    def _verify_refs(self, refs) -> list[dict]:
        """可见登记前逐引用核验——catalog 精确在册、固定版本可读、
        schema_hash／行数与引用声明一致。任何不符抛 ReleaseConflict，
        不产生可见发布。"""
        catalog = Catalog(self.root)
        checks = []
        for ref in refs:
            registered = catalog.get(
                ref.dataset_id, relative_uri=ref.relative_uri,
                lance_version=ref.lance_version,
            )
            if registered is None:
                raise ReleaseConflict(
                    "release table reference is not registered in the "
                    f"catalog: {ref.dataset_id} v{ref.lance_version}",
                )
            if registered != ref:
                raise ReleaseConflict(
                    "registered reference contradicts the release claim: "
                    f"{ref.dataset_id} v{ref.lance_version}",
                )
            try:
                dataset = ref.open(self.root)
            except Exception as exc:  # noqa: BLE001 - surfaced as conflict
                raise ReleaseConflict(
                    "release table unreadable at its pinned version: "
                    f"{ref.relative_uri} v{ref.lance_version} ({exc})",
                ) from exc
            actual_hash = lance_schema_hash(dataset.schema)
            actual_rows = dataset.count_rows()
            if actual_hash != ref.schema_hash or actual_rows != ref.row_count:
                raise ReleaseConflict(
                    "pinned release table drifted from its reference "
                    f"(schema/rows): {ref.relative_uri} v{ref.lance_version}",
                )
            checks.append({
                "dataset_id": ref.dataset_id,
                "lance_version": ref.lance_version,
                "schema_hash": actual_hash,
                "row_count": actual_rows,
                "catalog_registered": True,
                "pinned_readback": True,
            })
        return checks

    def register(self, release_id: str, *, release_kind: str, table_refs,
                 pipeline_run=None, validation=None,
                 previous_release_id=None) -> None:
        """登记一个发布；同 id 重复内容幂等，冲突报错。

        table_refs 为 DatasetRef（或其 dict）列表；登记即可见边界，因此每个
        引用先经 _verify_refs 核验（catalog 在册＋固定版本可读＋schema／
        行数一致），并把核验结果并入 validation 随发布冻结；不存在表、
        不存在版本、矛盾元数据在写入任何可见行之前失败。
        """
        refs = [ref if isinstance(ref, DatasetRef) else DatasetRef.from_dict(ref)
                for ref in table_refs]
        ref_checks = self._verify_refs(refs)
        if isinstance(validation, dict):
            validation = {**validation, "ref_checks": ref_checks}
        elif validation is None:
            validation = {"ref_checks": ref_checks}
        payload = json.dumps([ref.to_dict() for ref in refs],
                             ensure_ascii=False, sort_keys=True)
        row = {
            "release_id": release_id,
            "release_kind": release_kind,
            "pipeline_run": pipeline_run,
            "table_refs": payload,
            "validation": json.dumps(validation, ensure_ascii=False, sort_keys=True)
            if validation is not None else None,
            "status": "registered",
            "registered_at": _now_micros(),
            "previous_release_id": previous_release_id,
        }
        path = Path(self.uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = table_lock_path(path)
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ReleaseConflict("another writer holds the releases lock") from exc
            existing = self.get(release_id)
            if existing is not None:
                if all(existing[k] == row[k] for k in row if k != "registered_at"):
                    return
                raise ReleaseConflict(
                    f"release_id already registered with different content: {release_id}",
                )
            import pyarrow as pa
            import lance

            table = pa.Table.from_pylist([row], schema=_releases_schema())
            lance.write_dataset(table, self.uri, mode="append")


def write_registered_table(
    datasets_root, relative_uri: str, *, schema, schema_name: str,
    schema_version: str,
    rows_factory: Callable[[], Iterator[Mapping]],
    fingerprint: str,
    storage_options: Mapping[str, str] | None = None,
    max_rows_per_batch: int = 8192,
    register: bool = True,
    data_api=None,
    dataset_id: str | None = None,
) -> tuple[DatasetRef, int, bool]:
    """把 ``rows_factory`` 的行写为一张 Lance 表，登记并返回引用、行数与是否重放。

    组合 checkpoint_lance 的原子提交／重放契约与 catalog 登记；schema 显式
    传入（本层不持有任何业务 schema 集合）。行工厂必须是零参可重入迭代器
    （重放时由 checkpoint 直接读表，不再执行工厂）。

    重放（目标已有同 fingerprint 提交）时行工厂不执行——调用方需要源侧
    统计（对账、坏行计数）时须自行补一遍纯统计扫描。

    ``fingerprint`` 必须由输入身份派生（输入引用＋选择条件＋schema 版本），
    同位置同 fingerprint 重放不重执行；输入变化必须换 fingerprint 或新位置
    （checkpoint_lance 会拒绝同位置异 fingerprint）。
    """
    from .checkpoint import read_checkpoint_record

    if data_api is None:
        from ..standalone import local_data

        data_api = local_data()

    relative_path = Path(relative_uri)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError("relative_uri must stay within the datasets root")
    uri = str((Path(datasets_root) / relative_path).resolve(strict=False))

    existing = read_checkpoint_record(uri)
    if existing is not None:
        if existing["fingerprint"] != fingerprint:
            raise ValueError(
                "target already holds a different fingerprint; use a new location",
            )
        version = existing["committed_version"]
        rows = _verified_row_count(uri, version, existing)
    else:
        counter = {"rows": 0}

        def counting_rows():
            for row in rows_factory():
                counter["rows"] += 1
                yield row

        data_api.from_iter(counting_rows).checkpoint_lance(
            uri, schema=schema, fingerprint=fingerprint,
            storage_options=storage_options,
            max_rows_per_batch=max_rows_per_batch,
        )
        record = read_checkpoint_record(uri)
        if record is None:
            raise RuntimeError("Lance checkpoint sidecar missing after commit")
        version = record["committed_version"]
        rows = _verified_row_count(uri, version, record)
        if rows != counter["rows"]:
            raise RuntimeError("Lance 表行数与写入计数不一致")

    committed_schema = open_lance_dataset(uri, version, ()).schema
    if lance_schema_hash(committed_schema) != lance_schema_hash(schema):
        raise ValueError("requested schema differs from committed checkpoint schema")
    resolved_id = dataset_id if dataset_id is not None else _default_dataset_id(relative_uri)
    ref = DatasetRef(
        dataset_id=resolved_id,
        relative_uri=relative_uri,
        lance_version=version,
        schema_name=schema_name,
        schema_version=schema_version,
        schema_hash=lance_schema_hash(schema),
        row_count=rows,
    )
    if register:
        Catalog(datasets_root).register(ref)
    return ref, rows, existing is not None


def _default_dataset_id(relative_uri: str) -> str:
    return relative_uri[: -len(".lance")] if relative_uri.endswith(".lance") else relative_uri


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _verified_row_count(uri: str, version: int, record: Mapping) -> int:
    """按固定版本读回行数，并与 checkpoint 回执对账。"""
    dataset = open_lance_dataset(uri, version, ())
    rows = dataset.count_rows()
    if rows != record["row_count"]:
        raise RuntimeError("Lance checkpoint receipt row count drifted")
    return rows


__all__ = [
    "REGISTRY_RELATIVE", "RELEASES_RELATIVE", "Catalog", "CatalogConflict",
    "ReleaseConflict", "ReleaseRegistry", "write_registered_table",
]


def replace_registered_table(root, expected_ref: DatasetRef, table) -> DatasetRef:
    """Replace a registered table's content using an expected-version commit.

    Schema and logical identity remain unchanged; old pinned versions stay valid.
    Business validation and release selection belong to the caller.
    """
    import lance
    from ..errors import LanceWriteConflict
    registered = Catalog(root).get(expected_ref.dataset_id, relative_uri=expected_ref.relative_uri,
                                   lance_version=expected_ref.lance_version)
    if registered != expected_ref:
        raise ValueError('Expected replacement input must be registered exactly')
    current = expected_ref.open(root)
    uri = expected_ref.resolve(root)
    if lance.dataset(uri).version != expected_ref.lance_version:
        raise LanceWriteConflict('Table head changed before replacement')
    if not table.schema.equals(current.schema, check_metadata=True):
        raise ValueError('Replacement must preserve the table schema')
    fragments = []
    if table.num_rows:
        fragments.append(lance.fragment.LanceFragment.create(uri, table, schema=table.schema, mode='overwrite'))
    ds = lance.LanceDataset.commit(uri, lance.LanceOperation.Overwrite(table.schema, fragments),
                                  read_version=expected_ref.lance_version, max_retries=0,
                                  commit_message='demiflow: replace registered content')
    ref = DatasetRef(expected_ref.dataset_id, expected_ref.relative_uri, ds.version,
                     expected_ref.schema_name, expected_ref.schema_version,
                     lance_schema_hash(ds.schema), ds.count_rows(), store_id=expected_ref.store_id)
    Catalog(root).register(ref)
    return ref
