"""Batch 3 E2E verification: Forge independent managed runs without Workbench.

Verifies the complete standalone `demiflow forge run` lifecycle inside fresh
subprocesses where `demiflow.workbench` is explicitly blocked via `sys.meta_path`:
  1. `prepare` with explicit `--python` + `--read-root` + `--request-id` idempotency.
  2. `start`, competing start rejection, and `stop` at an active checkpoint.
  3. External B-class `local_ref` package upgrade (v1 -> v2) while stopped.
  4. Fresh process `show` + `resume --wait` by original `run_id`, running attempt 2
     from the original checkpoint and original v1 sealed package bytes.
  5. Workbench interoperability on the exact same project (reads the same run & attempts).
  6. Interrupted `.forge/index.sqlite` publication recovery via `rebuild-index`.
  7. Headless `demiflow forge run tools` / `invoke` without Workbench.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import lance
import pytest

REPO = Path(__file__).resolve().parents[1]


def _make_pkg(root: Path, files: dict[str, str]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding='utf-8')
    return root


def _init_project(root: Path) -> Path:
    from demiflow.forge.project import initialize_project
    initialize_project(root, name='headless_demo')
    for sub in ('flows', 'operators', 'configs', 'data'):
        (root / sub).mkdir(exist_ok=True)
    return root


def _run_forge_headless(args: list[str], *, domain: Path, expect_ok: bool = True) -> tuple[int, dict | str, str]:
    """Invoke `demiflow.forge.cli.forge` in a fresh subprocess with `demiflow.workbench` BLOCKED."""
    harness = (
        'import sys\n'
        'class _BlockWorkbench:\n'
        '    def find_spec(self, fullname, path, target=None):\n'
        '        if fullname == "demiflow.workbench" or fullname.startswith("demiflow.workbench."):\n'
        '            raise ImportError(f"Blocked Workbench import in headless Forge run: {fullname}")\n'
        '        return None\n'
        'sys.meta_path.insert(0, _BlockWorkbench())\n'
        'import click\n'
        'from demiflow.forge.cli import register\n'
        'cli = click.Group("demiflow")\n'
        'register(cli)\n'
        'cli.main(args=["forge", *sys.argv[1:]], prog_name="demiflow", standalone_mode=True)\n'
    )
    env = dict(os.environ)
    env['PYTHONPATH'] = str(REPO)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env['DEMIFLOW_FORGE_DOMAIN'] = str(domain)
    proc = subprocess.run(
        [sys.executable, '-c', harness, *args],
        cwd=str(REPO),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    stdout = proc.stdout.strip()
    try:
        parsed: dict | str = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        parsed = stdout
    if expect_ok:
        assert proc.returncode == 0, f'CLI failed (rc={proc.returncode}): args={args}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}'
    return proc.returncode, parsed, proc.stderr


def _build_resumable_project(tmp_path: Path) -> tuple[Path, Path, Path]:
    from _forge_project_fixtures import declare_existing_flow

    proj = _init_project(tmp_path / 'proj')
    ext = _make_pkg(tmp_path / 'ext_dep', {'__init__.py': 'ADD = 100\n'})
    (proj / 'pyproject.toml').write_text(
        '[tool.demiflow.packages]\n'
        f'ext_dep = {{source = "local_ref", path = "{ext}"}}\n',
        encoding='utf-8',
    )
    (proj / 'operators/step.py').write_text(
        'import time\n'
        'from pathlib import Path\n'
        'from ext_dep import ADD\n'
        'def enrich(row):\n'
        '    marker = Path(row["marker"])\n'
        '    if row["x"] == 2 and not marker.exists():\n'
        '        marker.write_text("seen", encoding="utf-8")\n'
        '        time.sleep(30)\n'
        '    return {"x": row["x"] + ADD, "marker": row["marker"]}\n',
        encoding='utf-8',
    )
    (proj / 'flows/pipe.py').write_text(
        'import pyarrow as pa\n'
        'from demiflow import data\n'
        'from operators.step import enrich\n'
        'def run(*, stage, target, marker):\n'
        '    schema = pa.schema([("x", pa.int64()), ("marker", pa.string())])\n'
        '    rows = data.from_items([{"x": 1, "marker": marker}, {"x": 2, "marker": marker}])\n'
        '    return (\n'
        '        rows.map(enrich)\n'
        '        .save_lance(stage, schema=schema, key="x", stage="s", mode="append", max_rows=2, max_index_bytes=8388608)\n'
        '        .write_lance(target, schema=schema, mode="create", return_receipt=True)\n'
        '    )\n',
        encoding='utf-8',
    )
    marker = proj / 'data/pause.flag'
    (proj / 'configs/pipe.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {
            'stage': str(proj / 'data/stage.lance'),
            'target': str(proj / 'data/target.lance'),
            'marker': str(marker),
        },
    }), encoding='utf-8')
    declare_existing_flow(proj, 'flows/pipe.py:run', params='configs/pipe.json')
    return proj, ext, marker


def test_forge_run_cli_headless_lifecycle_idempotency_upgrade_resume_and_workbench_interop(tmp_path, monkeypatch):
    domain = tmp_path / 'runtime-domain'
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(domain))
    proj, ext, marker = _build_resumable_project(tmp_path)

    # 1. Request-id generation & idempotent prepare in headless Forge (Workbench blocked)
    _, req_id_text, _ = _run_forge_headless(['run', 'request-id'], domain=domain)
    req_id = str(req_id_text).strip()
    assert req_id

    prep_args = [
        'run', 'prepare', str(proj), 'flows/pipe.py:run',
        '--params', 'configs/pipe.json',
        '--python', sys.executable,
        '--read-root', str(ext),
        '--limits', json.dumps({'cancel_grace_s': 1, 'timeout_s': 180}),
        '--request-id', req_id,
        '--exact-output',
        '--json',
    ]
    _, prep1, _ = _run_forge_headless(prep_args, domain=domain)
    assert isinstance(prep1, dict) and prep1['status'] == 'prepared', prep1
    run_id = prep1['run_id']
    digest = prep1['digest']

    # Repeat prepare with the exact same --request-id -> returns identical run_id & digest
    _, prep2, _ = _run_forge_headless(prep_args, domain=domain)
    assert isinstance(prep2, dict)
    assert prep2['run_id'] == run_id
    assert prep2['digest'] == digest

    # 2. Start attempt 1 headlessly, verify competing start is rejected, then stop at checkpoint
    _, started, _ = _run_forge_headless([
        'run', 'start', str(proj), run_id,
        '--expected', digest,
        '--python', sys.executable,
        '--read-root', str(ext),
        '--json',
    ], domain=domain)
    assert isinstance(started, dict) and started['run_id'] == run_id

    deadline = time.monotonic() + 30
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert marker.exists(), _run_forge_headless(['run', 'logs', str(proj), run_id, '--json'], domain=domain, expect_ok=False)

    # Competing start on an already-active run must NOT launch a second attempt
    rc_dup, dup_res, _ = _run_forge_headless([
        'run', 'start', str(proj), run_id,
        '--expected', digest,
        '--python', sys.executable,
        '--read-root', str(ext),
        '--json',
    ], domain=domain, expect_ok=False)
    assert isinstance(dup_res, dict)
    assert dup_res.get('attempt', 1) == 1
    assert dup_res.get('status') in {'starting', 'running', 'rejected'}

    # Competing start with stale/wrong expected attempt must be rejected
    rc_wrong, wrong_res, _ = _run_forge_headless([
        'run', 'start', str(proj), run_id,
        '--expected', digest,
        '--expected-attempt', '2',
        '--python', sys.executable,
        '--read-root', str(ext),
        '--json',
    ], domain=domain, expect_ok=False)
    assert rc_wrong != 0 and isinstance(wrong_res, dict) and wrong_res['status'] == 'rejected'

    # Stop attempt 1
    _run_forge_headless(['run', 'stop', str(proj), run_id, '--json'], domain=domain, expect_ok=False)
    stop_deadline = time.monotonic() + 30
    stopped_state = None
    while time.monotonic() < stop_deadline:
        _, st, _ = _run_forge_headless(['run', 'status', str(proj), run_id, '--json'], domain=domain, expect_ok=False)
        if isinstance(st, dict) and st.get('status') in {'cancelled', 'interrupted', 'failed'} and not st.get('supervisor_alive'):
            stopped_state = st
            break
        time.sleep(0.2)
    assert stopped_state is not None, 'Attempt 1 did not stop cleanly'
    assert stopped_state['status'] in {'cancelled', 'interrupted'}
    assert stopped_state['attempt'] == 1

    # 3. Upgrade external B-class local_ref package on disk (v1 ADD=100 -> v2 ADD=999)
    (ext / '__init__.py').write_text('ADD = 999\n', encoding='utf-8')

    # 4. In a brand-new process (still blocking Workbench), find run by run_id and resume
    _, found, _ = _run_forge_headless(['run', 'show', str(proj), run_id, '--json'], domain=domain, expect_ok=False)
    assert isinstance(found, dict) and found['run_id'] == run_id

    _, resumed, _ = _run_forge_headless([
        'run', 'resume', str(proj), run_id,
        '--expected', digest,
        '--expected-attempt', '1',
        '--wait',
        '--json',
    ], domain=domain)
    assert isinstance(resumed, dict) and resumed['status'] == 'succeeded', resumed
    assert resumed['attempt'] == 2

    # Output MUST reflect original sealed v1 bytes (ADD=100 -> [101, 102]), NOT v2 (ADD=999)!
    out_rows = sorted(lance.dataset(str(proj / 'data/target.lance')).to_table()['x'].to_pylist())
    assert out_rows == [101, 102]

    # Both attempts are traceable via headless Forge CLI
    _, listed, _ = _run_forge_headless(['run', 'list', str(proj), '--json'], domain=domain)
    assert any(item['run_id'] == run_id for item in listed['runs'])
    _, att1, _ = _run_forge_headless(['run', 'status', str(proj), run_id, '--attempt', '1', '--json'], domain=domain, expect_ok=False)
    _, att2, _ = _run_forge_headless(['run', 'status', str(proj), run_id, '--attempt', '2', '--json'], domain=domain)
    assert att1['attempt'] == 1 and att1['status'] in {'cancelled', 'interrupted'}
    assert att2['attempt'] == 2 and att2['status'] == 'succeeded'

    # 5. Open Workbench on the exact same project -> sees the exact same Run and attempts
    from demiflow.workbench.runs import view as wb_view
    wb_snap = wb_view.snapshot(proj, run_id)
    assert wb_snap['run_id'] == run_id
    assert wb_snap['status'] == 'succeeded'
    assert wb_snap['attempt'] == 2


def test_forge_run_index_interruption_rebuild_preserves_commits_and_attempts(tmp_path, monkeypatch):
    from _forge_project_fixtures import declare_existing_flow

    domain = tmp_path / 'runtime-domain'
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(domain))
    proj = _init_project(tmp_path / 'proj')
    (proj / 'flows/simple.py').write_text(
        'import pyarrow as pa\n'
        'from demiflow import data\n'
        'def run(*, target):\n'
        '    schema = pa.schema([("v", pa.int64())])\n'
        '    return data.from_items([{"v": 7}]).write_lance(target, schema=schema, mode="create", return_receipt=True)\n',
        encoding='utf-8',
    )
    (proj / 'configs/simple.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'target': str(proj / 'data/simple.lance')},
    }), encoding='utf-8')
    declare_existing_flow(proj, 'flows/simple.py:run', params='configs/simple.json')

    _, prep, _ = _run_forge_headless([
        'run', 'prepare', str(proj), 'flows/simple.py:run',
        '--params', 'configs/simple.json',
        '--exact-output',
        '--json',
    ], domain=domain)
    run_id = prep['run_id']
    _, done, _ = _run_forge_headless([
        'run', 'start', str(proj), run_id,
        '--expected', prep['digest'],
        '--wait',
        '--json',
    ], domain=domain)
    assert done['status'] == 'succeeded'

    # Simulate interrupted index publication: leave a real pending marker and remove index.sqlite
    from demiflow.forge import index as forge_index
    forge_index.begin(proj, 'run', run_id)
    idx_db = proj / '.forge/index.sqlite'
    if idx_db.exists():
        idx_db.unlink()

    # Rebuild index headlessly without Workbench
    _, rebuilt, _ = _run_forge_headless(['run', 'rebuild-index', str(proj), '--json'], domain=domain)
    assert isinstance(rebuilt, dict) and rebuilt.get('complete') is True and rebuilt.get('runs', 0) >= 1, rebuilt

    # Listing & showing the original run still exposes intact commit & attempt history
    _, listed, _ = _run_forge_headless(['run', 'list', str(proj), '--json'], domain=domain)
    assert any(item['run_id'] == run_id for item in listed['runs'])
    _, shown, _ = _run_forge_headless(['run', 'show', str(proj), run_id, '--json'], domain=domain)
    assert shown['status'] == 'succeeded'
    assert shown['attempt'] == 1
    assert shown['outputs'] and shown['outputs'][0]['status'] == 'committed'


def test_forge_run_tools_and_invoke_headless_without_workbench(tmp_path, monkeypatch):
    domain = tmp_path / 'runtime-domain'
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(domain))
    proj = _init_project(tmp_path / 'proj')

    _, tools_spec, _ = _run_forge_headless(['run', 'tools'], domain=domain)
    assert isinstance(tools_spec, list)
    names = {item['name'] for item in tools_spec}
    assert {'run.prepare', 'run.start', 'run.list', 'run.show', 'run.cancel', 'run.resume'} <= names

    _, listed, _ = _run_forge_headless([
        'run', 'invoke', str(proj), 'run.list', '--arguments', '{}', '--json',
    ], domain=domain)
    assert isinstance(listed, dict) and listed['runs'] == []
