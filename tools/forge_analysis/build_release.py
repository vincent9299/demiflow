#!/usr/bin/env python3
"""Build a matching intermediate helper/supervisor/worker from ONE wheel.

Run under the coordination test gate. This does not install or execute an
image, activate grants or certify B1. The integrated final source must rebuild
all artifacts again; predecessor helper02/image01 are not accepted inputs.
"""
import argparse
import hashlib
import json
import os
import platform
import sys
from pathlib import Path
import shutil
import zipfile

import build_helper
import build_image


def digest(path):
    return build_image.sha_file(Path(path))


def match(wheel, helper, image_lock):
    """Check every package member, including supervisor, against both outputs."""
    expected = {}
    with zipfile.ZipFile(wheel) as archive:
        for info in archive.infolist():
            if info.filename.startswith('demiflow/'):
                if info.file_size > build_helper.MAX_FILE:
                    raise ValueError('Wheel member too large for release matching')
                expected[info.filename] = hashlib.sha256(archive.read(info)).hexdigest()
    if len(expected) > build_helper.MAX_FILES or not expected:
        raise ValueError('Invalid release wheel member inventory')
    with zipfile.ZipFile(helper) as archive:
        names = {name for name in archive.namelist() if name.startswith('demiflow/')}
        if names != set(expected):
            raise ValueError('Helper and wheel member sets differ')
        for name, sha in expected.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != sha:
                raise ValueError('Helper source does not match wheel: '+name)
            image = image_lock['files'].get('usr/local/lib/python3.11/site-packages/'+name)
            if image is None or image['sha256'] != sha:
                raise ValueError('Worker source does not match wheel: '+name)
    if image_lock['wheel_sha256'] != digest(wheel):
        raise ValueError('Worker used another wheel')
    if 'demiflow/workbench/analysis/supervisor.py' not in expected:
        raise ValueError('Release lacks its matching supervisor')
    return hashlib.sha256(json.dumps(expected,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def build(wheel, output, *, baseline_sha256, media_lock=None):
    if platform.machine() != 'x86_64' or sys.version_info[:2] != (3, 11):
        raise ValueError('Fixed release recipe requires amd64 CPython3.11')
    if len(baseline_sha256) != 64 or any(char not in '0123456789abcdef' for char in baseline_sha256):
        raise ValueError('Explicit common baseline manifest SHA is required')
    wheel, output = Path(wheel).resolve(), Path(output)
    if output.exists() or not wheel.is_file() or wheel.stat().st_size > build_helper.MAX_TOTAL:
        raise ValueError('Existing bounded wheel and new private output directory required')
    parent=output.parent
    while not parent.exists(): parent=parent.parent
    if shutil.disk_usage(parent).free < 2*build_image.MAX_BYTES + 512*1024**2:
        raise ValueError('Insufficient reserved disk for worker and archive')
    output.mkdir(mode=0o700,parents=True)
    # Snapshot one explicit wheel before either build; all downstream inputs
    # use this copy and a second hash check rejects concurrent source mutation.
    sha=digest(wheel)
    copied=output/wheel.name
    shutil.copyfile(wheel,copied);copied.chmod(0o444)
    if digest(copied)!=sha or digest(wheel)!=sha:
        raise ValueError('Wheel changed while freezing release input')
    helper=output/'analysis-helper.pyz'
    helper_receipt=build_helper.build(copied,helper)
    (output/'helper-build.json').write_text(json.dumps(helper_receipt,indent=2)+'\n')
    selected=build_image.distributions()
    worker=output/'worker';worker.mkdir(mode=0o700)
    builder=build_image.Builder(worker)
    builder.interpreter();builder.packages(selected);builder.wheel(copied)
    if media_lock is not None:builder.media_tools(media_lock)
    builder.elf_dependencies()
    worker_receipt=builder.finish(worker,selected,copied)
    lock=json.loads((worker/'image-lock.json').read_bytes())
    source_digest=match(copied,helper,lock)
    with zipfile.ZipFile(copied) as archive:
        for source,target in [('resource-profile-v1.json','resource-profile.json'),('seccomp-v1.json','seccomp.json')]:
            (output/target).write_bytes(archive.read('demiflow/workbench/analysis/'+source))
    receipt=dict(format='demiflow_analysis_matched_release_v1',stage='intermediate_common_baseline',
        baseline_manifest_sha256=baseline_sha256,wheel_sha256=sha,package_source_sha256=source_digest,
        helper_sha256=helper_receipt['helper_sha256'],supervisor='same helper archive and wheel',
        builder_sha256={name:digest(Path(__file__).with_name(name)) for name in ('build_helper.py','build_image.py','build_release.py')},
        worker=worker_receipt,profile_sha256=digest(output/'resource-profile.json'),
        seccomp_sha256=digest(output/'seccomp.json'),media_tools=sorted(builder.media),
        activation_allowed=False,isolation_calibrated=False,merged_source_rebuild_required=True)
    (output/'release.json').write_text(json.dumps(receipt,indent=2)+'\n')
    return receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wheel',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--baseline-manifest-sha256',required=True)
    parser.add_argument('--media-lock',type=Path)
    args=parser.parse_args()
    print(json.dumps(build(args.wheel,args.output,baseline_sha256=args.baseline_manifest_sha256,
                           media_lock=args.media_lock),indent=2))


if __name__=='__main__':main()
