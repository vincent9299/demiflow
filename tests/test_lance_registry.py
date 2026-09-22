"""catalog 登记表 / releases 发布登记 / write_registered_table 契约测试。

通用示例（传感器遥测表）复用登记与发布 API——不包含任何知识／概念类
业务字段；平台测试不依赖任何业务仓库。
"""
import json

import pytest

pa = pytest.importorskip("pyarrow")
pytest.importorskip("lance")

from demiflow.lance.refs import DatasetRef
from demiflow.lance.registry import (
    Catalog, CatalogConflict, ReleaseConflict, ReleaseRegistry,
    write_registered_table,
)
from demiflow.lance.digest import table_digest_v1

SENSOR_SCHEMA = pa.schema([
    pa.field("sensor_id", pa.string(), nullable=False),
    pa.field("ts_us", pa.int64(), nullable=False),
    pa.field("celsius", pa.float64(), nullable=False),
])


def make_ref(dataset_id, lance_version, **overrides):
    fields = dict(
        dataset_id=dataset_id,
        relative_uri=f"telemetry/{dataset_id.split('/')[-1]}/out.lance",
        lance_version=lance_version,
        schema_name="sensor_readings",
        schema_version="v1",
        schema_hash="sha256:" + f"{lance_version:064d}"[-64:],
        row_count=10 * lance_version,
    )
    fields.update(overrides)
    return DatasetRef(**fields)


def sensor_rows(n):
    return iter({"sensor_id": "s1", "ts_us": i, "celsius": 20.0 + i}
                for i in range(n))


# ------------------------------------------------------------------ catalog

def test_register_and_read_back(tmp_path):
    catalog = Catalog(tmp_path)
    ref = make_ref("telemetry/x", 1)
    catalog.register(ref)
    assert catalog.latest("telemetry/x") == ref
    assert catalog.get("telemetry/x", relative_uri=ref.relative_uri,
                       lance_version=1) == ref
    assert catalog.latest("telemetry/missing") is None


def test_register_idempotent_on_identical_row(tmp_path):
    catalog = Catalog(tmp_path)
    ref = make_ref("telemetry/x", 1)
    catalog.register(ref)
    catalog.register(ref)
    assert len(catalog.registered()) == 1


def test_register_conflict_on_same_identity_different_content(tmp_path):
    catalog = Catalog(tmp_path)
    catalog.register(make_ref("telemetry/x", 1))
    with pytest.raises(CatalogConflict):
        catalog.register(make_ref("telemetry/x", 1, row_count=999))


def test_versions_ordered_by_registration_not_scan(tmp_path):
    catalog = Catalog(tmp_path)
    for version in (3, 1, 2):
        catalog.register(make_ref("telemetry/x", version))
    versions = catalog.versions("telemetry/x")
    assert [v.lance_version for v in versions] == [3, 1, 2]
    assert catalog.latest("telemetry/x").lance_version == 2


def test_empty_catalog_reads(tmp_path):
    catalog = Catalog(tmp_path / "empty-root")
    assert catalog.rows() == []
    assert catalog.registered() == []
    assert catalog.latest("anything") is None


def test_export_jsonl_snapshot(tmp_path):
    catalog = Catalog(tmp_path)
    catalog.register(make_ref("telemetry/x", 1))
    out = catalog.export(tmp_path / "datasets.jsonl")
    lines = out.read_text().strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["dataset_id"] == "telemetry/x"
    assert "registered_at" in payload


def test_registry_survives_reopen_and_mixes_datasets(tmp_path):
    catalog = Catalog(tmp_path)
    catalog.register(make_ref("telemetry/x", 1))
    catalog.register(make_ref("telemetry/y", 7))
    reopened = Catalog(tmp_path)
    assert {ref.dataset_id for ref in reopened.registered()} == {
        "telemetry/x", "telemetry/y",
    }


# ------------------------------------------------------- write_registered_table

def test_write_registered_table_fresh_register_and_readback(tmp_path):
    ref, rows, replayed = write_registered_table(
        tmp_path, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: sensor_rows(5), fingerprint="t1",
    )
    assert rows == 5 and replayed is False
    assert ref.row_count == 5 and ref.lance_version >= 1
    assert Catalog(tmp_path).get(ref.dataset_id, relative_uri=ref.relative_uri,
                                 lance_version=ref.lance_version) == ref
    # 固定版本打开（不追随 latest）
    dataset = ref.open(tmp_path)
    assert dataset.count_rows() == 5


def test_write_registered_table_replay_does_not_execute_factory(tmp_path):
    write_registered_table(
        tmp_path, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: sensor_rows(5), fingerprint="t1",
    )

    def refusing():
        raise AssertionError("replay must not execute the upstream factory")
        yield  # pragma: no cover

    ref, rows, replayed = write_registered_table(
        tmp_path, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=refusing, fingerprint="t1",
    )
    assert replayed is True and rows == 5


def test_write_registered_table_rejects_different_fingerprint(tmp_path):
    write_registered_table(
        tmp_path, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: sensor_rows(5), fingerprint="t1",
    )
    with pytest.raises(ValueError):
        write_registered_table(
            tmp_path, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
            schema_name="sensor_readings", schema_version="v1",
            rows_factory=lambda: sensor_rows(5), fingerprint="changed-input",
        )


def test_write_registered_table_survives_root_move(tmp_path):
    root1 = tmp_path / "root1"
    root1.mkdir()
    ref, _, _ = write_registered_table(
        root1, "telemetry/sensors/v1/out.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: sensor_rows(3), fingerprint="t1",
    )
    import shutil

    root2 = tmp_path / "root2"
    shutil.copytree(root1, root2)
    # 引用身份不变（相对位置），在搬迁后的根上按固定版本打开
    assert ref.open(root2).count_rows() == 3
    assert Catalog(root2).latest(ref.dataset_id) == ref


def test_write_registered_table_zero_rows_valid(tmp_path):
    ref, rows, replayed = write_registered_table(
        tmp_path, "telemetry/sensors/v1/empty.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: iter([]), fingerprint="empty",
    )
    assert rows == 0 and replayed is False and ref.row_count == 0


# ------------------------------------------------------------------ releases

def _write_table(root, relative="releases/telemetry/r1/out.lance", rows=2):
    def factory():
        return iter({"sensor_id": "s1", "ts_us": i, "celsius": 20.0 + i}
                    for i in range(rows))

    ref, count, _ = write_registered_table(
        root, relative, schema=SENSOR_SCHEMA, schema_name="sensor_readings",
        schema_version="v1", rows_factory=factory,
        fingerprint=f"r7:{relative}:{rows}",
    )
    assert count == rows
    return ref


def test_register_rejects_absent_table(tmp_path):
    ref = DatasetRef(dataset_id="releases/absent", relative_uri="releases/absent.lance",
                     lance_version=9, schema_name="sensor_readings",
                     schema_version="v1", schema_hash="sha256:x", row_count=77)
    with pytest.raises(ReleaseConflict):
        ReleaseRegistry(tmp_path).register("bad-release", release_kind="test",
                                           table_refs=[ref])
    assert ReleaseRegistry(tmp_path).get("bad-release") is None   # 无可见发布


def test_register_rejects_wrong_version_and_contradictions(tmp_path):
    good = _write_table(tmp_path)

    wrong_version = DatasetRef(**{**good.to_dict(), "lance_version": 99})
    with pytest.raises(ReleaseConflict):
        ReleaseRegistry(tmp_path).register("bad-v", release_kind="test",
                                           table_refs=[wrong_version])

    wrong_rows = DatasetRef(**{**good.to_dict(), "row_count": good.row_count + 5})
    with pytest.raises(ReleaseConflict):
        ReleaseRegistry(tmp_path).register("bad-rows", release_kind="test",
                                           table_refs=[wrong_rows])

    registry = ReleaseRegistry(tmp_path)
    assert registry.get("bad-v") is None and registry.get("bad-rows") is None


def test_register_freezes_verification_results(tmp_path):
    ref = _write_table(tmp_path)
    registry = ReleaseRegistry(tmp_path)
    registry.register("good-release", release_kind="test", table_refs=[ref],
                      pipeline_run="run-x")
    row = registry.get("good-release")
    assert row is not None and row["status"] == "registered"
    validation = json.loads(row["validation"])
    check = validation["ref_checks"][0]
    assert check["dataset_id"] == ref.dataset_id
    assert check["lance_version"] == ref.lance_version
    assert check["row_count"] == ref.row_count
    assert check["pinned_readback"] is True
    # 幂等重登记
    registry.register("good-release", release_kind="test", table_refs=[ref], pipeline_run="run-x")
    assert len([r for r in registry.rows() if r["release_id"] == "good-release"]) == 1


def test_generic_pipeline_reuses_registry_and_release(tmp_path):
    """无业务字段的示例：写入 → 登记摘要 → 发布 → 按固定引用读回。

    平台 API 的复用性验收（边界修订第 6 条）：一个不含知识／概念字段的
    例子能完整走 资产/登记/发布 API。
    """
    ref, rows, _ = write_registered_table(
        tmp_path, "telemetry/room101/v1/readings.lance", schema=SENSOR_SCHEMA,
        schema_name="sensor_readings", schema_version="v1",
        rows_factory=lambda: sensor_rows(9), fingerprint="room101",
        register=False,
    )
    # 登记时补内容摘要（表级 rowhash 聚合）
    digested = DatasetRef(**{
        **ref.to_dict(),
        "content_digest": table_digest_v1(
            {"sensor_id": r["sensor_id"], "ts_us": r["ts_us"], "celsius": r["celsius"]}
            for r in ref.open(tmp_path).to_table().to_pylist()
        ),
    })
    catalog = Catalog(tmp_path)
    catalog.register(digested)
    releases = ReleaseRegistry(tmp_path)
    releases.register("room101-2026q3", release_kind="telemetry",
                      table_refs=[digested], pipeline_run="room101-run")

    record = releases.get("room101-2026q3")
    assert record is not None
    pinned = DatasetRef.from_dict(json.loads(record["table_refs"])[0])
    table = pinned.open(tmp_path).to_table()
    assert table.num_rows == 9
    assert table.column("sensor_id").to_pylist() == ["s1"] * 9


def test_replay_rejects_changed_schema(tmp_path):
    ref = _write_table(tmp_path)
    with pytest.raises(ValueError, match="schema"):
        write_registered_table(tmp_path, ref.relative_uri,
            schema=pa.schema([pa.field("different", pa.string())]),
            schema_name="different", schema_version="v1", rows_factory=lambda: iter([]),
            fingerprint=f"r7:{ref.relative_uri}:2")


def test_release_same_id_cannot_silently_change_validation(tmp_path):
    ref = _write_table(tmp_path)
    registry = ReleaseRegistry(tmp_path)
    registry.register("fixed", release_kind="test", table_refs=[ref], validation={"approved": False})
    with pytest.raises(ReleaseConflict):
        registry.register("fixed", release_kind="test", table_refs=[ref], validation={"approved": True})
