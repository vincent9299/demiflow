"""Real private SQLite CAS/receipt/history behavior in a dedicated test process."""
from contextlib import closing, contextmanager
import hashlib
import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys

import pytest

from demiflow.workbench.analysis import contracts as c

ROOT = Path(__file__).resolve().parents[1]
MIB = 1024*1024


class Controller:
    def __init__(self, process):
        self.process = process

    def read(self):
        with selectors.DefaultSelector() as poll:
            poll.register(self.process.stdout, selectors.EVENT_READ)
            assert poll.select(10), "controller did not respond"
        line = self.process.stdout.readline()
        assert line, self.process.stderr.read()
        return json.loads(line)

    def send(self, operation, **payload):
        data = json.dumps(dict(operation=operation, **payload), ensure_ascii=False).encode() + b"\n"
        assert len(data) <= 65536
        self.process.stdin.write(data); self.process.stdin.flush()

    def call(self, operation, **payload):
        self.send(operation, **payload)
        return self.read()


@contextmanager
def controller(directory, *, database_bytes=16*MIB, wal_bytes=8*MIB):
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(ROOT))
    process = subprocess.Popen([sys.executable, "-B", "-u", str(Path(__file__).with_name("_forge_analysis_records_controller.py")), str(directory), str(database_bytes), str(wal_bytes)],
                               cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    client = Controller(process)
    try:
        assert client.read() == {"ready": True}
        yield client
    finally:
        if process.poll() is None:
            process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
        if process.returncode not in {0, 71, -9}:
            pytest.fail(process.stderr.read().decode())
        process.stdout.close(); process.stderr.close()


def document():
    return {"title": "分析", "cells": [{"id": "first", "language": "python", "source": "print('source is stored, never executed')"}]}


def source_manifest():
    return {"kind": "fixed_lance", "binding": "a"*64, "retention": "external", "rows": 2,
            "case_key": {"kind": "row_id", "field": None},
            "schema": [{"name": "input", "type": "utf8", "nullable": False}, {"name": "output", "type": "utf8", "nullable": True}]}


def question():
    return {"id": "quality", "title": "可用吗", "type": "choice", "required": True,
            "options": [{"id": "yes", "label": "可用"}, {"id": "no", "label": "不可用"}]}


def view(source):
    return {"version": 1, "source_snapshot": source["digest"], "case_key": source["case_key"],
            "fields": [{"id": "in", "field": "input", "label": "输入", "role": "input", "format": "text"},
                       {"id": "out", "field": "output", "label": "输出", "role": "output", "format": "md"}],
            "grouping": [], "filters": None, "sort": [], "layout": "card", "questions": [question()]}


def save(client, source, *, request_id="answer-1", definition=None, revision=0, value="yes", controls=None):
    return client.call("save_annotations", request_id=request_id,
        target={"source": source["digest"], "case_key": "18446744073709551615", "variant": None, "field": None},
        entries=[{"definition": definition or question(), "expected_revision": revision, "action": "set", "value": value}],
        origin={"kind": "unsaved_view", "reference": "draft-one"}, controls=controls or {})


def test_notebook_picker_names_come_from_current_saved_revision_without_rewriting_history(tmp_path):
    with controller(tmp_path/'db') as client:
        initial=client.call('create_notebook',request_id='one',document=document())['result']
        listed=client.call('list_documents',kind='notebook')['result']['items']
        assert listed==[dict(id=initial['id'],revision=1,title=document()['title'])]
        client.call('patch_notebook',request_id='rename',notebook_id=initial['id'],expected_revision=1,patch=dict(title='Case analysis'))
        assert client.call('list_documents',kind='notebook')['result']['items']==[dict(id=initial['id'],revision=2,title='Case analysis')]
        assert client.call('notebook',notebook_id=initial['id'],revision=1)['result']['document']==document()


def test_notebook_response_loss_cas_and_history_survive_controller_crash(tmp_path):
    with controller(tmp_path/"db") as client:
        client.send("create_crash", request_id="lost-create", document=document())
        assert client.process.wait(timeout=10) == 71
    with controller(tmp_path/"db") as client:
        receipt = client.call("receipt", request_id="lost-create")["result"]
        repeat = client.call("create_notebook", request_id="lost-create", document=document())["result"]
        assert repeat["id"] == receipt["id"] and repeat["replayed"]
        assert client.call("create_notebook", request_id="lost-create", document={**document(), "title": "changed"})["code"] == "conflict"
        identifier = receipt["id"]
        first = client.call("patch_notebook", request_id="edit-1", notebook_id=identifier, expected_revision=1, patch={"title": "new title"})
        assert first["result"]["revision"] == 2
        second = client.call("patch_notebook", request_id="edit-2", notebook_id=identifier, expected_revision=1, patch={"title": "other browser"})
        assert second["code"] == "conflict"
        assert client.call("notebook", notebook_id=identifier, revision=1)["result"]["document"] == document()
        assert client.call("notebook", notebook_id=identifier)["result"]["document"]["title"] == "new title"
        assert client.call("receipt", request_id="edit-2")["code"] == "not_found"
        assert client.call("receipt", request_id="lost-create", project="w"+"1"*32)["code"] == "not_found"


def test_permission_revoked_before_commit_rolls_back_document_and_receipt(tmp_path):
    with controller(tmp_path/"db") as client:
        result = client.call("create_notebook", request_id="denied", document=document(), controls={"revoke_at_check": 2})
        assert result["code"] == "operation_rejected"
        assert client.call("list_documents", kind="notebook")["result"]["items"] == []
        assert client.call("receipt", request_id="denied")["code"] == "not_found"


def test_saved_views_and_annotations_are_independent_definition_history_is_immutable(tmp_path):
    with controller(tmp_path/"db") as client:
        source = client.call("register_source", manifest=source_manifest())["result"]
        descriptor = view(source)
        saved = client.call("save_view", request_id="view-1", view_id=None, expected_revision=0, descriptor=descriptor)["result"]
        first = save(client, source)["result"]
        changed = {**question(), "title": "图片可用吗"}
        next_answer = save(client, source, request_id="new-definition", definition=changed, value="no")["result"]
        assert first["heads"][0]["definition"] != next_answer["heads"][0]["definition"]
        assert first["view_saved"] is False and next_answer["view_saved"] is False
        still_old = client.call("view", view_id=saved["id"])["result"]
        assert still_old["revision"] == 1 and still_old["document"] == descriptor
        for definition, expected in ((question(), "yes"), (changed, "no")):
            history = client.call("annotations", target=first["target"], definition=c.digest(definition))["result"]
            assert history["items"][0]["value"] == expected and history["definition"] == definition
        assert save(client, source, request_id="other-browser", value="no")["code"] == "conflict"
        # Source disappearance prevents edits, but not interpretation of history.
        assert save(client, source, request_id="gone", revision=1, controls={"source_valid": False})["code"] == "conflict"
        history = client.call("annotations", target=first["target"], definition=c.digest(question()), controls={"source_valid": False})["result"]
        assert history["items"][0]["value"] == "yes"
        assert client.call("receipt", request_id="gone")["code"] == "not_found"


def test_annotation_batch_cas_is_atomic_and_revert_appends_history(tmp_path):
    with controller(tmp_path/"db") as client:
        source = client.call("register_source", manifest=source_manifest())["result"]
        first = save(client, source)["result"]
        target = first["target"]
        extra = {"id": "note", "title": "备注", "type": "text", "required": False}
        entries = [{"definition": extra, "expected_revision": 0, "action": "set", "value": "must roll back"},
                   {"definition": question(), "expected_revision": 0, "action": "set", "value": "no"}]
        failed = client.call("save_annotations", request_id="atomic-conflict", target=target, entries=entries, origin={"kind": "unsaved_view", "reference": "draft"})
        assert failed["code"] == "conflict"
        assert client.call("annotations", target=target, definition=c.digest(extra))["code"] == "not_found"
        save(client, source, request_id="answer-2", revision=1, value="no")
        reverted = client.call("save_annotations", request_id="undo", target=target,
            entries=[{"definition": question(), "expected_revision": 2, "action": "revert", "revision": 1}],
            origin={"kind": "unsaved_view", "reference": "draft"})["result"]
        assert reverted["heads"][0]["revision"] == 3
        history = client.call("annotations", target=target, definition=c.digest(question()), limit=2)["result"]
        assert [item["value"] for item in history["items"]] == ["yes", "no"]
        older = client.call("annotations", target=target, definition=c.digest(question()), before=history["next_before"], watermark=history["annotation_watermark"])["result"]
        assert [item["revision"] for item in older["items"]] == [1]
        # A pinned annotation watermark does not reinterpret new saves.
        original = client.call("annotations", target=target, definition=c.digest(question()), watermark=first["annotation_watermark"])["result"]
        assert [item["revision"] for item in original["items"]] == [1]


def test_backup_preserves_receipts_and_unknown_schema_is_not_modified(tmp_path):
    directory = tmp_path/"db"
    with controller(directory) as client:
        saved = client.call("create_notebook", request_id="one", document=document())["result"]
        backup = client.call("backup")["result"]
    backup_path = Path(backup["path"])
    with closing(sqlite3.connect(backup_path.as_uri()+"?mode=ro", uri=True)) as db:
        assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert json.loads(db.execute("SELECT result FROM operation_receipts").fetchone()[0])["id"] == saved["id"]
    database_path = directory/"records.sqlite"
    assert not (directory/"backup.sqlite-wal").exists() and not (directory/"backup.sqlite-shm").exists()
    with closing(sqlite3.connect(database_path)) as db:
        db.execute("PRAGMA user_version=999")
    before = hashlib.sha256(database_path.read_bytes()).hexdigest()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(ROOT))
    result = subprocess.run([sys.executable, "-B", str(Path(__file__).with_name("_forge_analysis_records_controller.py")), str(directory), str(16*MIB), str(8*MIB)], cwd=ROOT, env=env, input=b"", capture_output=True, timeout=10)
    assert result.returncode != 0 and b"Unknown analysis database version" in result.stderr
    assert hashlib.sha256(database_path.read_bytes()).hexdigest() == before
    assert directory.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in directory.iterdir())
