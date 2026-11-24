"""OS facts survive the actual data-only error envelope, including two hops."""
import errno
import json

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_failures import failure_payload, restore_failure


@pytest.mark.parametrize('original', [
    OSError(errno.ENOSPC, 'disk full'),
    OSError(errno.EIO, 'zero-progress write', '/tmp/input', None, '/tmp/output'),
    FileNotFoundError(errno.ENOENT, 'missing', b'/tmp/input-\xff', None, b'/tmp/output-\xfe'),
    PermissionError(errno.EACCES, 'denied', '/tmp/owned'),
    TimeoutError(errno.ETIMEDOUT, 'native timeout'),
])
def test_os_error_facts_survive_json_service_and_worker_hops(original):
    current = original
    for _ in range(2):
        try:
            raise current
        except OSError as error:
            current = restore_failure(json.loads(json.dumps(failure_payload(error))))
        assert type(current) is type(original)
        assert current.errno == original.errno
        assert current.strerror == original.strerror
        assert current.filename == original.filename
        assert current.filename2 == original.filename2
        assert current.args == original.args and str(current) == str(original)


def test_legacy_message_only_os_error_and_known_group_keep_their_surfaces():
    original = OSError('message without an OS number')
    payload = failure_payload(original)
    assert 'os_error' not in payload
    restored = restore_failure(payload)
    assert type(restored) is OSError and restored.errno is None and restored.args == original.args
    group = ExceptionGroup('storage', [OSError(errno.EIO, 'short write'), original])
    for _ in range(2):
        group = restore_failure(json.loads(json.dumps(failure_payload(group))))
    assert group.exceptions[0].errno == errno.EIO
    assert group.exceptions[1].args == original.args


@pytest.mark.parametrize('changed', [
    {'errno': True}, {'errno': '28'}, {'errno': 1 << 80},
    {'strerror': []}, {'strerror': 'x' * 2049},
    {'filename': 'x' * 4097}, {'filename': {'bytes_hex': '0'}},
    {'filename': {'bytes_hex': 'gg'}}, {'filename': {'bytes_hex': '00' * 4097}},
    {'filename': {'bytes_hex': '00', 'constructor': 'remote'}},
    {'extra': 'not a native field'},
])
def test_forged_os_fields_cannot_escape_the_bounded_data_contract(changed):
    fields = {'errno': errno.ENOSPC, 'strerror': 'disk full', **changed}
    with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
        restore_failure({'error_type': 'builtins.OSError', 'message': 'disk full', 'os_error': fields})


def test_unknown_os_subclasses_are_not_promoted_or_imported():
    class BusinessStorageError(OSError):
        pass
    payload = failure_payload(BusinessStorageError(errno.ENOSPC, 'custom storage failure'))
    assert payload['error_type'] is None and 'os_error' not in payload
    assert type(restore_failure(payload)) is DataflowError
    payload.update(error_type='untrusted_business.module.OSError',
                   os_error={'errno': errno.ENOSPC, 'strerror': 'not trusted'})
    assert type(restore_failure(payload)) is DataflowError


def test_unbounded_optional_filename_does_not_drop_the_original_errno():
    original = OSError(errno.ENOSPC, 'disk full', 'x' * 4097)
    payload = failure_payload(original)
    assert 'filename' not in payload['os_error']
    assert len(json.dumps(payload)) < 5000
    restored = restore_failure(payload)
    assert restored.errno == errno.ENOSPC and restored.strerror == 'disk full'
    assert restored.filename is None
