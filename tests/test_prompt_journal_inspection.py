"""Read-only pages enforce admission before decoding JSON."""
import sqlite3

import pytest
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def test_paged_inspection_keeps_pending_errors_and_byte_limits(tmp_path):
    path = tmp_path / 'calls.sqlite'
    writer = SQLitePromptJournal(path)
    for i in range(5):
        req = {'i': i, 'text': '汉' * (1000 if i == 3 else 1)}
        writer.reserve(req)
        if i < 4:
            writer.response(req, {'body': i})
        else:
            writer.failed(req, RuntimeError('failed'), 0.1)
    writer.close()
    before = path.read_bytes()
    reader = SQLitePromptJournal(path, read_only=True)
    assert reader.inspection_stats() == {'requests': 5, 'responses': 4, 'saved_errors': 1, 'without_response_or_error': 0}
    page = reader.call_page(limit=2, max_record_bytes=200, max_page_bytes=300)
    assert page[0]['omitted'] == 'inspection_byte_limit'
    assert page[0]['stored_bytes'] > 3000  # UTF-8 bytes, not characters.
    assert page[1]['request']['i'] == 2
    assert [r['request']['i'] for r in reader.call_page(offset=2, limit=2)] == [1, 0]
    assert reader.call_page(responses_only=False, limit=1)[0]['error']['detail'] == 'failed'
    assert reader.call_page(offset=10) == []
    with pytest.raises(ValueError):
        reader.call_page(limit=100)
    reader.close()
    assert path.read_bytes() == before
    with sqlite3.connect(path) as db:
        assert db.execute('select count(*) from calls').fetchone()[0] == 5
