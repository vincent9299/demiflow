"""Reuse real provider equivalence and fault proofs with a bounded owned cluster."""
import pytest

ray = pytest.importorskip('ray')
from test_u2_chain_ray import test_sem_local_ray_equivalence
from test_u3_graph_ray import test_sem_local_ray_graph_equivalence
from test_u3_graph_ray import test_ray_graph_consumer_death_recovers_whole_graph_exactly_once


@pytest.fixture(scope='module')
def ray_cluster(tmp_path_factory):
    import tempfile
    import shutil
    root = tempfile.mkdtemp(prefix='demiflow-ray-')  # Unix socket path < 108 bytes.
    ray.init(num_cpus=8, object_store_memory=128 * 1024**2, _memory=8 * 1024**3,
             _temp_dir=root, include_dashboard=False,
             logging_level='ERROR')
    try:
        yield
    finally:
        ray.shutdown()
        shutil.rmtree(root)
