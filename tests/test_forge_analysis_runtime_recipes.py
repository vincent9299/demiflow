"""Prepared external recipes fail closed; never build FFmpeg or probe cgroups."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tarfile
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from demiflow.workbench.analysis import media_worker
from demiflow.forge.schema import ForgeError

ROOT=Path(__file__).resolve().parents[1]


def script(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'tools/forge_analysis'/f'{name}.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_missing_output_lane_refuses_before_any_host_reader(monkeypatch):
    monkeypatch.setitem(sys.modules,'demiflow.workbench.analysis.output_worker',None)
    with pytest.raises(ForgeError,match='absent'):media_worker.configuration({})


def test_media_sandbox_refusal_precedes_import_and_output_parsing(monkeypatch):
    def refuse(role):
        assert role=='media'
        raise ForgeError('sandbox unavailable')
    run=Mock()
    monkeypatch.setattr(media_worker,'require_job',refuse)
    monkeypatch.setitem(sys.modules,'demiflow.workbench.analysis.output_worker',SimpleNamespace(run=run))
    with pytest.raises(ForgeError,match='sandbox unavailable'):media_worker.main()
    run.assert_not_called()


def test_ffprobe_recipe_rejects_wrong_digest_and_archive_symlinks(tmp_path):
    builder=script('build_ffprobe')
    archive=tmp_path/'input.tar.xz'
    with tarfile.open(archive,'w:xz') as out:
        entry=tarfile.TarInfo('ffmpeg-8.1.3/configure');entry.type=tarfile.SYMTYPE;entry.linkname='/bin/sh';out.addfile(entry)
    with pytest.raises(ValueError,match='hash'):
        builder.extract(archive,tmp_path/'bad','0'*64)
    with pytest.raises(ValueError,match='member'):
        builder.extract(archive,tmp_path/'bad',hashlib.sha256(archive.read_bytes()).hexdigest())
    assert not (tmp_path/'bad').exists()


def test_resource_sampler_refuses_ungranted_template_before_output_or_proc(tmp_path):
    sampler=script('capture_resources')
    binding=tmp_path/'binding.json'
    binding.write_text(json.dumps(dict(format='demiflow_analysis_executor_binding_v2',binding_status='TEMPLATE_NOT_GRANTED',
        activation=dict(calibration_allowed=False))))
    output=tmp_path/'observations.jsonl'
    with pytest.raises(ValueError,match='authorization'):sampler.capture(binding,output,1)
    assert not output.exists()


def test_ffprobe_detects_silently_disabled_opus_and_unrequested_network_protocol(tmp_path):
    builder=script('build_ffprobe')
    header='#define CONFIG_FFPROBE 1\n#define CONFIG_SWRESAMPLE 1\n'
    header+=''.join('#define CONFIG_'+name+' 0\n' for name in ('FFMPEG','FFPLAY','NETWORK','AVDEVICE','AVFILTER'))
    (tmp_path/'config.h').write_text(header)
    components=''.join('#define CONFIG_'+name+'_'+kind+' 1\n' for kind,names in builder.COMPONENTS.items() for name in sorted(names))
    path=tmp_path/'config_components.h';path.write_text(components.replace('CONFIG_OPUS_DECODER 1','CONFIG_OPUS_DECODER 0'))
    with pytest.raises(ValueError,match='OPUS_DECODER'):builder.verify_configuration(tmp_path)
    path.write_text(components+'#define CONFIG_HTTP_PROTOCOL 1\n')
    with pytest.raises(ValueError,match='HTTP_PROTOCOL'):builder.verify_configuration(tmp_path)
    path.write_text(components)
    assert builder.verify_configuration(tmp_path)['verified'] is True
    # Upstream repeats CONFIG_LIBSMBCLIENT 0 in its generated config.h.
    (tmp_path/'config.h').write_text(header+'#define CONFIG_LIBSMBCLIENT 0\n'*2)
    assert builder.verify_configuration(tmp_path)['verified'] is True
    (tmp_path/'config.h').write_text(header+'#define CONFIG_SWRESAMPLE 0\n')
    with pytest.raises(ValueError,match='Conflicting'):builder.verify_configuration(tmp_path)
