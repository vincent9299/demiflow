"""Exchange selection and the current artifact reader safety boundary."""
from dataclasses import replace
import pytest
from demiflow.planning.dag import select_exchange
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_published_gc import put

def test_mode_selection_is_traits_driven():
    assert select_exchange({"input_mode": "finite_scope"})[0] == "finite_sealed"
    assert select_exchange({"input_mode": "incremental"})[0] == "memory_incremental"
    assert select_exchange({"input_mode": "incremental"}, durability_declared="durable")[0] == "durable_incremental"


@pytest.mark.parametrize('key', ['../escape.pkl', '/etc/passwd'])
def test_artifact_reader_rejects_path_escape(tmp_path, key):
    with store_fixture(tmp_path) as (_, store, _):
        ref = put(store, b'original')
        with pytest.raises(DataflowError, match='ArtifactRef.key'):
            list(store.read(replace(ref, key=key), max_bytes=100))

def test_artifact_reader_rejects_other_domain(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        ref = put(store, b'original')
        with pytest.raises(DataflowError, match='another store domain'):
            list(store.read(replace(ref, store_id='other-store'), max_bytes=100))

def test_artifact_reader_rejects_changed_original_bytes(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        ref = put(store, b'original')
        store._object_path(ref).write_bytes(b'altered!')
        with pytest.raises(DataflowError, match='bytes do not match'):
            list(store.read(ref, max_bytes=100))

def test_artifact_reader_rejects_symlink(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        ref = put(store, b'original')
        path = store._object_path(ref)
        outside = tmp_path / 'outside'
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
        with pytest.raises(DataflowError, match='symlink'):
            list(store.read(ref, max_bytes=100))
