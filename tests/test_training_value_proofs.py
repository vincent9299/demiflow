from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow.execution.training.inputs import prove_lance


def proof(root, table, *, media=(), media_roots=()):
    lance.write_dataset(table, str(root))
    bounds = SimpleNamespace(max_row_bytes=1 << 20, max_state_bytes=1 << 20)
    return prove_lance(str(root), 1, {'x': 'int64'}, source_digest='a'*64,
                      check_stop=lambda: None, bounds=bounds, media=media, media_roots=media_roots)


def test_training_hashes_accessible_columns_order_and_schema_metadata(tmp_path):
    table = pa.table({'x': [1], 'y': [2]})
    a = proof(tmp_path / 'a', table)
    assert a.identity() == proof(tmp_path / 'same', table).identity()
    for label, variant in [('extra', pa.table({'x': [1], 'y': [7]})),
                           ('order', table.select(['y', 'x'])),
                           ('metadata', table.replace_schema_metadata({'unit': 'cm'}))]:
        assert proof(tmp_path / label, variant).identity() != a.identity()


def test_media_is_a_declared_authorized_input(tmp_path):
    import hashlib
    root = tmp_path / 'media'
    root.mkdir()
    media = tmp_path / 'outside'
    media.write_bytes(b'content')
    value = {'uri': str(media), 'sha256': hashlib.sha256(b'content').hexdigest()}
    table = pa.table({'x': [1], 'attachment': [value]})
    assert proof(tmp_path / 'ordinary', table).rows == 1
    with pytest.raises(ValueError, match='outside'):
        proof(tmp_path / 'named', table, media=['attachment'], media_roots=[str(root)])
