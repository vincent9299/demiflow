"""Read-only replay cannot mutate evidence or start a provider/service."""
import asyncio
import sqlite3
import pytest
from demiflow import data
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from demiflow.operator_llm.journal import request_key, UncertainPromptCall
from test_map_prompt_async import PACK, prompt, server


def test_read_only_journal_preserves_imported_metadata_and_rejects_mutation(tmp_path):
    path = tmp_path / 'evidence.sqlite'
    writer = SQLitePromptJournal(path)
    request = {'a': 1}
    pending = {'a': 2}
    writer.reserve(request)
    writer.response(request, {'body': 'saved'})
    writer.reserve(pending)
    writer.close()
    with sqlite3.connect(path) as db:
        db.execute('UPDATE calls SET request_json=NULL WHERE request_key=?', (request_key(request),))
    before = path.read_bytes(), path.stat().st_mtime_ns
    reader = SQLitePromptJournal(path, read_only=True)
    assert reader.lookup(request) == {'body': 'saved'}
    assert reader.lookup({'a': 3}) is None
    with pytest.raises(UncertainPromptCall):
        reader.lookup(pending)
    for action in [lambda: reader.reserve({'a': 4}), lambda: reader.response(request, {}),
                   lambda: reader.requeue_uncertain([request_key(pending)], actor='test', reason='test'),
                   lambda: reader.import_lance(root=tmp_path, relative_uri='old.lance', version=1)]:
        with pytest.raises(PermissionError, match='read-only'):
            action()
    reader.close()
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    missing = tmp_path / 'missing/calls.sqlite'
    with pytest.raises(sqlite3.OperationalError):
        SQLitePromptJournal(missing, read_only=True).lookup(request)
    assert not missing.parent.exists()


def test_readonly_runtime_never_starts_service_even_without_zero_budget(server, tmp_path):
    path = tmp_path / 'calls.sqlite'
    options = {'sqlite_journal': {'path': str(path)}}
    prompt(data.from_items([{'item': 'saved'}]), options=options).materialize()
    import os
    import sys
    from demiflow.services import ManagedHTTPService
    # A passive service declaration at the saved endpoint. Starting it fails on the occupied
    # port (the fixture server) or, failing that, its process leaves a marker.
    started = tmp_path / 'service-started'
    script = tmp_path / 'service.py'
    script.write_text('import pathlib, sys\npathlib.Path(sys.argv[1]).touch()\nraise SystemExit(3)\n')
    endpoint = os.environ['TEST_PROMPT_URL']  # the endpoint the saved calls were made to
    service = ManagedHTTPService([sys.executable, str(script), str(started)], base_url=endpoint,
                                 root=tmp_path / 'service', startup_timeout_s=5, shutdown_timeout_s=.5,
                                 readiness_timeout_s=.2, poll_interval_s=.01,
                                 resource_directory=tmp_path / 'service-ports')
    result = prompt(data.from_items([{'item': 'saved'}, {'item': 'new'}]),
                    options={'sqlite_journal': {'path': str(path), 'read_only': True}},
                    error_output='error', service=service).materialize().take_all()
    by_item = {r['item']: r for r in result}
    assert by_item['saved']['answer'] == 'ok'
    assert by_item['new']['error']['type'] == 'PromptReplayMissError'
    assert len(server['requests']) == 1
    assert not started.exists()
