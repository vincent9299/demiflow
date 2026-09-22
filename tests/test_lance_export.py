import lance
import pyarrow as pa
import pytest

from demiflow.lance.export import export_snapshot


def test_export_fixed_snapshot_reuse_and_failure_cleanup(tmp_path):
    ds = lance.write_dataset(pa.table({"value": [1]}), str(tmp_path / "a.lance"))
    def write(handle):
        handle.write(str(ds.to_table()["value"][0].as_py()))
    first = export_snapshot(ds, tmp_path / "cache", projection="v1", suffix=".txt", write=write)
    def fail(handle):
        handle.write("partial")
        raise RuntimeError("interrupted")
    assert export_snapshot(ds, tmp_path / "cache", projection="v1", suffix=".txt", write=fail) == first
    head = lance.write_dataset(pa.table({"value": [2]}), ds.uri, mode="overwrite")
    with pytest.raises(RuntimeError):
        export_snapshot(head, tmp_path / "cache", projection="v1", suffix=".txt", write=fail)
    assert first.read_text() == "1"
    assert not list((tmp_path / "cache").glob("*.partial"))
