"""Explicit media provenance and exact wheel/helper/worker matching; no OCI."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile

import pytest

from test_forge_analysis_helper_builder import wheel
from test_forge_analysis_image_builder import builder as image_builder

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools/forge_analysis'))
try: import build_release as release
finally: sys.path.pop(0)


def test_matched_release_refuses_stale_worker_or_missing_supervisor(tmp_path):
    source=tmp_path/'one.whl';wheel(source)
    helper=tmp_path/'helper.pyz';release.build_helper.build(source,helper)
    with zipfile.ZipFile(source) as archive:
        files={'usr/local/lib/python3.11/site-packages/'+name:dict(sha256=hashlib.sha256(archive.read(name)).hexdigest())
               for name in archive.namelist() if name.startswith('demiflow/')}
    lock=dict(files=files,wheel_sha256=release.digest(source))
    assert len(release.match(source,helper,lock))==64
    lock['files']['usr/local/lib/python3.11/site-packages/demiflow/workbench/analysis/supervisor.py']['sha256']='0'*64
    with pytest.raises(ValueError,match='Worker source'):release.match(source,helper,lock)


def media_lock(tmp_path):
    binary=tmp_path/'ffprobe';binary.write_bytes(b'\x7fELFfixture-not-an-executable');binary.chmod(0o555)
    notice=tmp_path/'license';notice.write_bytes(b'Fixture license only')
    item=dict(name='ffprobe',version='fixture-1',path=str(binary),sha256=release.digest(binary),
        license_path=str(notice),license_sha256=release.digest(notice),configuration='fixture only; never execute')
    lock=tmp_path/'media.json';lock.write_text(json.dumps(dict(format='demiflow_analysis_media_inputs_v1',
        review_ref='unit fixture, no production approval',tools=[item])))
    return lock,item


def test_only_explicit_hashed_media_and_license_enter_private_image(tmp_path):
    lock,item=media_lock(tmp_path)
    builder=image_builder.Builder(tmp_path/'image')
    builder.media_tools(lock)
    assert (builder.root/'usr/local/bin/ffprobe').read_bytes()==Path(item['path']).read_bytes()
    assert builder.media['ffprobe']['version']=='fixture-1'
    assert builder.files['usr/local/bin/ffprobe']['mode']==0o555
    assert builder.media_lock_sha256==release.digest(lock)


@pytest.mark.parametrize('failure',['digest','name','script','license'])
def test_unreviewed_tool_never_falls_back_to_host_path(tmp_path,failure):
    lock,item=media_lock(tmp_path)
    value=json.loads(lock.read_bytes())
    if failure=='digest':value['tools'][0]['sha256']='0'*64
    elif failure=='name':value['tools'][0]['name']='shell'
    elif failure=='license':value['tools'][0]['license_sha256']='0'*64
    else:
        Path(item['path']).write_bytes(b'#!/bin/sh\nexit 0\n')
        value['tools'][0]['sha256']=release.digest(item['path'])
    lock.write_text(json.dumps(value))
    builder=image_builder.Builder(tmp_path/'image')
    with pytest.raises(ValueError):builder.media_tools(lock)
    assert not builder.files
