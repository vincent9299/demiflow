"""Deterministic pure-Python archive/resource loading; not helper deployment."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("analysis_helper_builder",ROOT/"tools/forge_analysis/build_helper.py")
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)


def wheel(path,*,hook=False):
    files={"demiflow/__init__.py":b"", "demiflow/workbench/__init__.py":b"", "demiflow/workbench/analysis/__init__.py":b"",
        "demiflow/workbench/analysis/__main__.py":b"from importlib.resources import files\ndef main():\n print(files(__package__).joinpath('resource-profile-v1.json').read_text()); return 0\n",
        "demiflow/workbench/analysis/controller.py":b"# unit packaging fixture only\n",
        "demiflow/workbench/analysis/watchdog_service.py":b"# unit packaging fixture only\n",
        "demiflow/workbench/analysis/supervisor.py":b"def main(): print('supervisor-fixture'); return 0\n",
        "demiflow/workbench/analysis/resource-profile-v1.json":b'{"unit_only":true}',
        "demiflow/workbench/analysis/seccomp-v1.json":b'{}'}
    if hook:files["demiflow/unreviewed.pth"]=b"unreviewed startup"
    with zipfile.ZipFile(path,"w") as output:
        for name,raw in files.items():output.writestr(name,raw)
    # Publish the test input before its immutable-input identity is captured.
    with path.open("rb") as source:
        os.fsync(source.fileno())


def test_helper_archive_reproducible_and_resources_work_without_site_packages(tmp_path):
    source=tmp_path/"candidate.whl";wheel(source)
    one=tmp_path/"one.pyz";two=tmp_path/"two.pyz"
    first=builder.build(source,one);second=builder.build(source,two)
    assert first["helper_sha256"]==second["helper_sha256"]
    assert first["activation_allowed"] is False and first["execution_tested"] is False
    result=subprocess.run([sys.executable,"-I","-S","-B",str(one)],capture_output=True,timeout=5,check=True)
    assert json.loads(result.stdout)=={"unit_only":True}


def test_startup_hook_or_existing_destination_refused_before_release(tmp_path):
    source=tmp_path/"candidate.whl";wheel(source,hook=True)
    with pytest.raises(ValueError,match="hooks"):
        builder.build(source,tmp_path/"refused.pyz")
    assert not (tmp_path/"refused.pyz").exists()
    target=tmp_path/"existing.pyz";target.write_bytes(b"original")
    with pytest.raises(ValueError):builder.build(source,target)
    assert target.read_bytes()==b"original"
