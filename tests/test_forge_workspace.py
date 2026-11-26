"""Workspace acceptance uses owned Lance/image fixtures and the real worker API."""

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import socket
import threading
import urllib.error
import urllib.request

from click.testing import CliRunner
import lance
import pyarrow as pa
import pytest

from demiflow.cli import main
from demiflow.forge.table_bindings import make_resolver
from demiflow.forge.runs import manager as runs

from demiflow.workbench.projects import workspace as api
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.worker import operation
from demiflow.workbench.api import WorkspaceServer
from demiflow.workbench.api import export_report
from demiflow.workbench.projects.workspace import source_text


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    source = Path(__file__).parents[1] / "examples/forge_workspace/make_demo.py"
    spec = importlib.util.spec_from_file_location("forge_demo_fixture", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path_factory.mktemp("forge-demo") / "workspace"
    result = module.create(root)
    return root, result["reports"]


@pytest.fixture
def isolated(demo, tmp_path):
    root = tmp_path / "workspace"
    shutil.copytree(demo[0], root)
    return root


def attach(root, alias, rows, schema):
    lance.write_dataset(
        pa.Table.from_pylist(rows, schema=schema), str(root / "data" / f"{alias}.lance")
    )
    return project_ops.attach_table(
        root,
        alias,
        relative_uri=f"{alias}.lance",
        version=1,
        dataset_id=alias,
        schema_name=alias,
        schema_version="1",
    )


def test_real_workspace_inspection_variants_and_case_reads(demo):
    root, reports = demo
    context = api.workspace_context(root)
    assert len(context["tables"]) == 3 and context["execution"] == "local_checked_v3"
    assert context["standard"]["version"] == "2.4.0"
    assert {r["id"] for r in context["standard"]["rules"]} >= {
        "FL-06",
        "OP-06",
        "OP-07",
        "OP-08",
    }
    for digest in reports.values():
        result = project_ops.report_status(root, digest)
        assert result["status"] == "checked", result["changes"]
        report = result["report"]
        assert len(report["nodes"]) == 7 and len(report["edges"]) == 6
        assert report["coverage"]["operator_behavior"] == "not_run"
    assert not (root / "data/selected_a.lance").exists()
    page = project_ops.sample_table(
        root, "candidates", columns=["case_id", "question"], limit=1
    )
    assert (
        page["version"] == 3
        and page["rows"][0]["case_id"] == "c01"
        and page["has_more"]
    )
    assert set(page["rows"][0]) == {"case_id", "question", "_rowid"}
    detail = project_ops.sample_table(root, "candidates", row_id=int(page["rows"][0]["_rowid"]))
    assert detail["rows"][0]["question"].strip() == "三角形的面积是多少？"
    assert (
        project_ops.sample_table(root, "candidates", offset=2, limit=1)["rows"][0]["case_id"]
        == "c03"
    )
    diff = api.diff_reports(root, reports["a"], reports["b"])
    assert not diff["same_graph"]
    assert (
        diff["arguments"]["left"]["minimum"] == 0.8
        and diff["arguments"]["right"]["minimum"] == 0.9
    )


def test_preview_binding_pins_alias_across_detail_media_and_cli(isolated):
    root = isolated
    description = project_ops.describe_table(root, "candidates")
    expected = description["binding"]["digest"]
    assert description["columns"][:3] == ["case_id", "question", "image_uri"]
    assert set(description["columns"]) == set(description["metadata"]["fields"])
    page = project_ops.sample_table(root, "candidates", columns=["case_id", "image_uri"], expected_binding=expected)
    assert page["binding"] == description["binding"]
    row_id = int(page["rows"][0]["_rowid"])
    media = operation("media", root, alias="candidates", row_id=row_id, field="image_uri", expected_binding=expected)
    assert media["binding"]["digest"] == expected
    result = CliRunner().invoke(main, ["workbench", "table", "show", str(root), "candidates", "--expected-binding", expected, "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["binding"]["digest"] == expected
    control = root / "tables/candidates.json"
    replacement = json.loads((root / "tables/comparison_fixture.json").read_text())
    replacement["alias"] = "candidates"
    control.write_text(json.dumps(replacement))
    for action in [lambda: project_ops.describe_table(root, "candidates", expected_binding=expected),
                   lambda: project_ops.sample_table(root, "candidates", row_id=row_id, expected_binding=expected),
                   lambda: operation("media", root, alias="candidates", row_id=row_id, field="image_uri", expected_binding=expected)]:
        with pytest.raises(ForgeError, match="binding changed"):
            action()
    result = CliRunner().invoke(main, ["workbench", "table", "sample", str(root), "candidates", "--expected-binding", expected, "--json"])
    assert result.exit_code != 0 and "binding changed" in result.output
    assert project_ops.describe_table(root, "candidates")["binding"]["digest"] != expected


def test_preview_identity_includes_location_and_source_hash(isolated, tmp_path):
    import hashlib

    root = isolated
    control, ref, uri = api.table_binding(root, api.load_workspace(root)[1], "candidates")
    first = api.preview_binding(root, control, uri)
    assert api.preview_binding(root, control, tmp_path / "moved.lance")["digest"] != first["digest"]
    for invalid in ["", "g" * 64, True, 123]:
        with pytest.raises(ForgeError, match="digest"):
            project_ops.sample_table(root, "candidates", expected_binding=invalid)
    path = "operators/question_rules.py"
    source = root / path
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    assert source_text(root, path, expected_sha256=expected)["text"] == source.read_text()
    source.write_text(source.read_text() + "\n# changed\n")
    with pytest.raises(ForgeError, match="Source changed"):
        source_text(root, path, expected_sha256=expected)


def test_preview_uses_the_binding_it_checked_when_alias_changes(isolated):
    import os
    import subprocess
    import sys

    root = isolated
    expected = project_ops.describe_table(root, "candidates")["binding"]["digest"]
    script = '''
import json
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.worker import main
original = ws.table_binding
def replace_after_resolution(root, workspace, alias):
    result = original(root, workspace, alias)
    replacement = json.loads((root / 'tables/comparison_fixture.json').read_text())
    replacement['alias'] = alias
    (root / 'tables' / (alias + '.json')).write_text(json.dumps(replacement))
    return result
ws.table_binding = replace_after_resolution
main()
'''
    result = subprocess.run([sys.executable, "-B", "-c", script], text=True, capture_output=True,
        input=json.dumps({"operation": "sample", "root": str(root), "arguments": {"alias": "candidates", "columns": ["case_id", "question"], "expected_binding": expected}}), timeout=60,
        env={**os.environ, "TOKIO_WORKER_THREADS": "2", "RAYON_NUM_THREADS": "2", "LANCE_CPU_THREADS": "2", "CUDA_VISIBLE_DEVICES": "",
             "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"})
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["ok"], value
    assert value["value"]["version"] == 3 and value["value"]["binding"]["digest"] == expected
    assert value["value"]["rows"][0]["question"].strip() == "三角形的面积是多少？"


def test_explicit_matching_keeps_duplicate_and_missing_keys(demo):
    root, _ = demo
    rows = project_ops.sample_table(
        root, "comparison_fixture", match={"field": "case_id", "value": "c02"}
    )["rows"]
    assert len(rows) == 2 and rows[0]["question"] != rows[1]["question"]
    assert (
        project_ops.sample_table(
            root, "comparison_fixture", match={"field": "case_id", "value": "c03"}
        )["rows"]
        == []
    )
    assert (
        project_ops.sample_table(
            root,
            "comparison_fixture",
            match={"field": "case_id", "value": "x' OR true --"},
        )["rows"]
        == []
    )
    with pytest.raises(ForgeError, match="Unknown preview column: missing"):
        project_ops.sample_table(root, "candidates", match={"field": "missing", "value": "c01"})


def test_inspection_respects_inherited_address_space_limit(demo):
    import subprocess
    import sys

    root, _ = demo
    script = '''
import os, resource, sys
sys.path.insert(0, sys.argv[1])
import pyarrow as pa
import lance
pa.set_cpu_count(2)
pa.set_io_thread_count(2)
with open('/proc/self/statm') as stream:
    baseline = int(stream.read(128).split()[0]) * os.sysconf('SC_PAGE_SIZE')
ceiling = baseline + 192 * 1024**2
resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
from demiflow.workbench.worker import main
main()
assert resource.getrlimit(resource.RLIMIT_AS) == (ceiling, ceiling)
'''
    request = {"operation": "describe", "root": str(root / "data/candidates.lance"),
               "arguments": {"version": 3}}
    result = subprocess.run([sys.executable, "-I", "-B", "-c", script,
                             str(Path(__file__).resolve().parents[1])],
        input=json.dumps(request), text=True, capture_output=True, timeout=60,
        env={"PATH": "/usr/bin:/bin", "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1",
             "MALLOC_ARENA_MAX": "2", "TOKIO_WORKER_THREADS": "2", "RAYON_NUM_THREADS": "2",
             "LANCE_CPU_THREADS": "2", "CUDA_VISIBLE_DEVICES": ""})
    assert result.returncode == 0, result.stderr
    response = json.loads(result.stdout)
    assert response["ok"], response
    assert response["value"]["version"] == 3
    assert response["value"]["row_count"] == 3


def test_images_header_budget_and_paths(isolated):
    root = isolated
    row_id = int(
        project_ops.sample_table(root, "candidates", columns=["case_id"], limit=1)["rows"][0][
            "_rowid"
        ]
    )
    result = operation(
        "media", root, alias="candidates", row_id=row_id, field="image_uri"
    )
    assert result["mime"] == "image/jpeg" and result["width"] == 640
    assert "table version alone" in result["media_identity"]
    schema = pa.schema([("image_uri", pa.string())])
    for index, path in enumerate(
        ["../../escape.png", "/etc/passwd", "https://example.test/a.png"]
    ):
        alias = "path" + str(index)
        attach(root, alias, [{"image_uri": path}], schema)
        with pytest.raises(ForgeError, match="root|path|Path"):
            operation("media", root, alias=alias, row_id=0, field="image_uri")
    from PIL import Image

    large = root / "data/media/large.png"
    # Highly compressible fixture: encoded bytes fit, header pixel count does not.
    Image.new("1", (5000, 4000)).save(large)
    attach(root, "largeimage", [{"image_uri": "media/large.png"}], schema)
    with pytest.raises(ForgeError, match="pixel budget"):
        operation("media", root, alias="largeimage", row_id=0, field="image_uri")
    oversized = root / "data/media/encoded.png"
    with oversized.open("wb") as output:
        output.truncate(8 * 1024**2 + 1)
    attach(root, "encodedimage", [{"image_uri": "media/encoded.png"}], schema)
    with pytest.raises(ForgeError, match="encoded size"):
        operation("media", root, alias="encodedimage", row_id=0, field="image_uri")
    (root / "data/media/link.png").symlink_to(large)
    attach(root, "symlinkimage", [{"image_uri": "media/link.png"}], schema)
    with pytest.raises(ForgeError, match="[Ss]ymlink"):
        operation("media", root, alias="symlinkimage", row_id=0, field="image_uri")


def test_projection_and_byte_budgets_before_python_conversion(isolated):
    root = isolated
    attach(
        root,
        "wide",
        [{"id": 1, "huge": "x" * (2 * 1024**2)}],
        pa.schema([("id", pa.int64()), ("huge", pa.string())]),
    )
    assert project_ops.sample_table(root, "wide", columns=["id"])["rows"][0]["id"] == 1
    with pytest.raises(ForgeError, match="budget"):
        project_ops.sample_table(root, "wide", columns=["huge"])
    with pytest.raises(ForgeError, match="budget"):
        project_ops.sample_table(root, "wide", columns=["huge"], row_id=0)
    with pytest.raises(ForgeError, match="page"):
        project_ops.sample_table(root, "wide", limit=101)
    from demiflow.lance.preview import describe_table

    with pytest.raises(RuntimeError, match="isolated worker"):
        describe_table(root / "data/wide.lance", 1)


def test_attach_head_replace_detach_and_readonly_locations(isolated, tmp_path):
    root = isolated
    external = tmp_path / "external"
    external.mkdir()
    uri = external / "input.lance"
    lance.write_dataset(pa.table({"id": [1]}), str(uri))
    api.add_location(root, "external", external)
    with pytest.raises(ForgeError, match="require dataset_id"):
        project_ops.attach_table(
            root,
            "source",
            relative_uri="input.lance",
            version=1,
            location_id="external",
        )
    attached = project_ops.attach_table(
        root,
        "source",
        relative_uri="input.lance",
        version=None,
        location_id="external",
        dataset_id="independent",
        schema_name="input",
        schema_version="1",
    )
    assert attached["table"]["ref"]["lance_version"] == 1
    lance.write_dataset(pa.table({"id": [2]}), str(uri), mode="overwrite")
    assert project_ops.sample_table(root, "source")["rows"][0]["id"] == 1
    with pytest.raises(ForgeError, match="changed"):
        project_ops.attach_table(
            root,
            "source",
            relative_uri="input.lance",
            version=2,
            location_id="external",
            dataset_id="independent",
            schema_name="input",
            schema_version="1",
            expected="wrong",
        )
    replaced = project_ops.attach_table(
        root,
        "source",
        relative_uri="input.lance",
        version=2,
        location_id="external",
        dataset_id="independent",
        schema_name="input",
        schema_version="1",
        expected=attached["digest"],
    )
    assert project_ops.sample_table(root, "source")["rows"][0]["id"] == 2
    assert not api.detach_table(root, "source", expected=replaced["digest"])[
        "data_deleted"
    ]
    assert uri.exists()
    params = json.loads((root / "flows/select_a.json").read_text())
    params["arguments"]["target"]["location_id"] = "external"
    with pytest.raises(ForgeError, match="workspace-owned"):
        project_ops.inspect_flow(root, "flows/questions.py:select", params=params)


def test_report_binding_relocation_changes_and_unrelated_flow(isolated):
    root = isolated
    old = api.workspace_context(root)["reports"][0]["digest"]
    assert "workspace_root_changed" in project_ops.report_status(root, old)["changes"]
    report = project_ops.inspect_flow(
        root, "flows/questions.py:select", params="flows/select_a.json"
    )
    digest = report["digest"]
    assert report["status"] == "checked"
    (root / "flows/unrelated.py").write_text('raise RuntimeError("not a dependency")\n')
    assert project_ops.report_status(root, digest)["status"] == "checked"
    operator = root / "operators/question_rules.py"
    operator.write_text(operator.read_text() + "\n# changed\n")
    status = project_ops.report_status(root, digest)
    assert status["status"] == "stale" and any(
        c.startswith("source_changed:") for c in status["changes"]
    )
    assert api.load_report(root, digest) == report
    params = root / "flows/select_a.json"
    value = json.loads(params.read_text())
    value["arguments"]["minimum"] = 0.6
    params.write_text(json.dumps(value))
    assert "parameters_changed" in project_ops.report_status(root, digest)["changes"]
    (root / "data/candidates.lance").rename(root / "data/moved.lance")
    assert any(
        c.startswith("table_unavailable:")
        for c in project_ops.report_status(root, digest)["changes"]
    )


def test_report_tamper_and_atomic_disk_failure(isolated, monkeypatch):
    root = isolated
    digest = api.workspace_context(root)["reports"][0]["digest"]
    path = root / ".forge/inspections" / f"{digest}.json"
    value = json.loads(path.read_text())
    value["status"] = "invented"
    path.write_text(json.dumps(value))
    with pytest.raises(ForgeError, match="modified"):
        api.load_report(root, digest)
    target = root / "tables/failed.json"

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(api.os, "link", fail)
    with pytest.raises(OSError, match="disk full"):
        api.write_control(target, {}, create=True)
    assert not target.exists() and not list(target.parent.glob(".forge-write-*"))


def test_store_quota_and_concurrent_parameter_change(isolated, monkeypatch):
    root = isolated
    # Control edits during the second platform comparison cannot publish success.
    from demiflow.lance import preview
    from demiflow.forge import flow_check

    monkeypatch.setattr(
        preview, "_guard", lambda: None
    )  # trusted small fixture, same-process race injection only
    original = flow_check.FlowChecker.inspect

    def raced(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        path = root / "flows/select_a.json"
        value = json.loads(path.read_text())
        value["arguments"]["minimum"] = 0.5
        path.write_text(json.dumps(value))
        return result

    monkeypatch.setattr(flow_check.FlowChecker, "inspect", raced)
    before = set((root / ".forge/inspections").iterdir())
    with pytest.raises(ForgeError, match="changed"):
        api._inspect_flow(
            root, "flows/questions.py:select", params="flows/select_a.json",
            table_resolver=make_resolver(api.table_binding, runs.result_table),
        )
    assert set((root / ".forge/inspections").iterdir()) == before
    monkeypatch.setattr(flow_check.FlowChecker, "inspect", original)
    from demiflow.forge import index as catalog

    with api.control_lock(root):
        for number in range(catalog.MAX_PENDING):
            catalog.begin(root, 'report', format(number, '064x'))
    with pytest.raises(ForgeError, match="pending budget.*full"):
        project_ops.inspect_flow(
            root, "flows/questions.py:select", params="flows/select_b.json"
        )


def test_cli_matches_python_and_rejects_new_workspace_in_legacy_entry(demo):
    root, reports = demo
    runner = CliRunner()
    value = runner.invoke(
        main,
        [
            "workbench",
            "table",
            "sample",
            str(root),
            "candidates",
            "--column",
            "case_id",
            "--limit",
            "1",
        ],
    )
    assert value.exit_code == 0, value.output
    assert json.loads(value.output) == project_ops.sample_table(
        root, "candidates", columns=["case_id"], limit=1
    )
    bad = runner.invoke(
        main,
        [
            "workbench",
            "flow",
            "inspect",
            str(root),
            "flows/missing.py:run",
            "--params",
            "flows/select_a.json",
        ],
    )
    assert bad.exit_code == 1 and json.loads(bad.output)["status"] == "incomplete"
    old = runner.invoke(main, ["forge", "check", str(root)])
    assert old.exit_code != 0
    attach = runner.invoke(
        main, ["workbench", "table", "attach", str(root), "x", "--uri", "candidates.lance"]
    )
    assert attach.exit_code == 2


def test_server_auth_origin_source_and_readonly_protocol(demo):
    root, reports = demo
    server = WorkspaceServer(root)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def request(path, headers=None, data=None):
        req = urllib.request.Request(
            server.origin + path, headers=headers or {}, data=data
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                return response.status, response.read(), response.headers
        except urllib.error.HTTPError as error:
            return error.code, error.read(), error.headers

    auth = {"X-Forge-Token": server.token}
    try:
        assert request("/")[0] == 200
        assert request("/api/context")[0] == 403
        assert request('/api/changes')[0] == 403
        assert request('/api/changes', {**auth, 'Origin': 'https://outside.test'})[0] == 403
        code, body, _ = request('/api/changes', auth)
        assert code == 200 and json.loads(body)['workspace'] == str(root)
        assert request('/api/changes?execute=true', auth)[0] == 400
        assert (
            request("/api/context", {**auth, "Origin": "https://outside.test"})[0]
            == 403
        )
        assert request("/api/context", {**auth, "Host": "outside.test"})[0] == 403
        # SSH can change both the local port and the browser's loopback name.
        for host in ('localhost:55971', '127.0.0.1:55971', '[::1]:55971'):
            forwarded = {**auth, 'Host': host, 'Origin': 'http://' + host}
            assert request('/', {'Host': host})[0] == 200
            assert request('/api/context', forwarded)[0] == 200
            # Reaching request validation proves same-origin POST is admitted.
            assert request('/api/inspect', forwarded, b'{"entry":"x"}')[0] == 400
            assert request('/api/context', {'Host': host})[0] == 403
            assert request('/api/context', {**forwarded, 'Origin': 'http://localhost:55972'})[0] == 403
        for host in ('localhost.evil.test:55971', 'localhost:0', 'localhost:65536', '127.0.0.1@evil.test', '127.0.0.2:55971'):
            assert request('/', {'Host': host})[0] == 403
        code, body, headers = request("/api/context", auth)
        assert code == 200 and json.loads(body)["name"] == "question-experiments"
        assert (
            headers["Cache-Control"] == "no-store"
            and headers["X-Content-Type-Options"] == "nosniff"
        )
        assert request("/api/source?path=../../etc/passwd", auth)[0] == 400
        assert request("/api/execute", auth, b"{}")[0] == 404
        for path in ('run-prepare', 'run-start', 'run-cancel', 'run-reconcile', 'run-attach'):
            assert request('/api/' + path, auth, b'{}')[0] == 403
        server.enable_runs = True
        forwarded = {**auth, 'Host': 'localhost:55971', 'Origin': 'http://localhost:55971'}
        assert request('/api/run-prepare', forwarded, b'{}')[0] == 400
        assert request('/api/run-start', {**forwarded, 'Origin': 'http://localhost:55972'}, b'{}')[0] == 403
        server.enable_runs = False
        assert request("/api/inspect", auth, b'{"entry":"x"}')[0] == 400
        assert request("/api/source?path=operators/question_rules.py", auth)[0] == 200
        description = json.loads(request('/api/table?alias=candidates', auth)[1])
        binding = description['binding']['digest']
        code, body, _ = request('/api/sample?alias=candidates&limit=1&expected_binding=' + binding, auth)
        assert code == 200 and json.loads(body)['binding']['digest'] == binding
        row_id = json.loads(body)['rows'][0]['_rowid']
        code, body, headers = request('/api/media?alias=candidates&field=image_uri&row_id=' + row_id + '&expected_binding=' + binding, auth)
        assert code == 200 and body and headers['X-Forge-Binding'] == binding
        for suffix in ('&expected_binding=', '&expected_binding=' + '0' * 64, '&unexpected=1'):
            expected_status = 409 if suffix == '&expected_binding=' + '0' * 64 else 400
            assert request('/api/table?alias=candidates' + suffix, auth)[0] == expected_status
        assert request('/api/source?path=operators/question_rules.py&expected_sha256=', auth)[0] == 400
        # Queue capacity is checked before spawning another connection thread.
        acquired = [server.requests.acquire(timeout=3) for _ in range(10)]
        try:
            assert request("/api/context", auth)[0] == 503
        finally:
            for held in acquired:
                if held:
                    server.requests.release()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_offline_export_has_no_samples_and_escapes_source_payload(demo, tmp_path):
    root, reports = demo
    out = tmp_path / "report.html"
    value = export_report(root, reports["a"], out)
    assert value["samples_included"] is False and value["current_binding"] == "not_live"
    text = out.read_text()
    assert "window.FORGE_EXPORT=" in text
    assert 'src="/app.js"' not in text and 'href="/style.css"' not in text
    assert 'src="/view.js"' not in text and 'src="/codex.js"' not in text
    assert 'document.modelContext' not in text
    assert "三角形的面积是多少" not in text
    assert "operator_behavior" in text and "not_run" in text
    with pytest.raises(FileExistsError):
        export_report(root, reports["a"], out)
    with pytest.raises(ForgeError):
        source_text(root, "README.md")


def test_native_directory_links_and_file_budgets(isolated):
    root = isolated
    from demiflow.lance.preview import _open

    with pytest.raises(ValueError, match="file count"):
        _open(root / "data/candidates.lance", 3, maximum_files=1)
    # Unused history is not traversed; the actual fixed-version files are checked.
    table = root / 'data/candidates.lance'
    (table / 'unused-history-link').symlink_to(root / 'data/scores.lance')
    assert project_ops.sample_table(root, 'candidates', columns=['case_id'])['rows']
    filename = lance.dataset(str(table), version=3).get_fragments()[0].data_files()[0].path
    data_file = table / 'data' / filename
    moved = root / 'owned-fixture-copy.lance'
    data_file.rename(moved)
    data_file.symlink_to(moved)
    with pytest.raises(ForgeError, match="Symlink"):
        project_ops.sample_table(root, "candidates", columns=["case_id"])
