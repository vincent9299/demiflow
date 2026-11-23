import pytest
from demiflow.execution.native_queue.queue_sql import require_local_storage


@pytest.mark.parametrize('filesystem', ['nfs4', 'dpc'])
def test_network_disk_rejected_before_creation(tmp_path, filesystem):
    mountinfo=tmp_path/'mountinfo';mountinfo.write_text(f'1 0 0:1 / / rw - {filesystem} server:/ rw\n')
    with pytest.raises(ValueError,match='local storage'):
        require_local_storage(tmp_path/'queue.sqlite',mountinfo=mountinfo)
    assert not (tmp_path/'queue.sqlite').exists()
