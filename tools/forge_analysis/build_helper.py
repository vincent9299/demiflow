#!/usr/bin/env python3
"""Deterministic stdlib-only helper zipapp from one exact DemiFlow wheel.

No install, shared environment mutation, execution, extraction or network. The
outer supervisor launches with reviewed Python ``-I -S -B helper.pyz``; native
libraries/Jupyter remain in the separately reviewed worker image/namespace.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import zipfile

MAX_FILES = 2048
MAX_TOTAL = 64*1024*1024
MAX_FILE = 16*1024*1024
ENTRY = (b"import sys\n"
         b"if len(sys.argv) > 1 and sys.argv[1] in ('supervisor', 'supervisor-child'):\n"
         b"    from demiflow.workbench.analysis.supervisor import main\n"
         b"else:\n"
         b"    from demiflow.workbench.analysis.__main__ import main\n"
         b"raise SystemExit(main())\n")


def build(wheel, destination):
    wheel, destination = Path(wheel), Path(destination)
    if not wheel.is_file() or wheel.stat().st_size > MAX_TOTAL or destination.exists():
        raise ValueError("Bounded explicit wheel and a new helper path are required")
    before = wheel.stat()
    wheel_hash = hashlib.sha256()
    with wheel.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024), b""):
            wheel_hash.update(chunk)
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    total = count = 0
    required = {"demiflow/workbench/analysis/__main__.py", "demiflow/workbench/analysis/controller.py",
        "demiflow/workbench/analysis/watchdog_service.py", "demiflow/workbench/analysis/supervisor.py", "demiflow/workbench/analysis/resource-profile-v1.json", "demiflow/workbench/analysis/seccomp-v1.json"}
    files = {}
    with zipfile.ZipFile(wheel) as archive:
        members = archive.infolist()
        if len(members) > MAX_FILES:
            raise ValueError("Wheel member count exceeds helper bound")
        names = set()
        for member in members:
            path = PurePosixPath(member.filename)
            if (path.is_absolute() or str(path) != member.filename or ".." in path.parts or len(path.parts) > 32
                    or member.filename in names or member.is_dir() or len(member.filename) > 1024):
                raise ValueError("Invalid/duplicate wheel member")
            names.add(member.filename)
            mode = member.external_attr >> 16
            if (stat.S_ISLNK(mode) or member.file_size > MAX_FILE or member.flag_bits & 1
                    or member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}):
                raise ValueError("Unsupported wheel member type/encoding/size")
            if not member.filename.startswith("demiflow/"):
                continue
            if path.suffix in {".pyc", ".pth", ".so"} or "__pycache__" in path.parts:
                raise ValueError("Wheel cannot inject startup/native hooks into the stdlib helper")
            total += member.file_size; count += 1
            if total > MAX_TOTAL:
                raise ValueError("Helper input byte budget exceeded")
        if not required <= names:
            raise ValueError("Wheel lacks the current concrete helper entrypoints/resources")
        with zipfile.ZipFile(destination, mode="x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as output:
            def put(name, raw):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED; info.external_attr = (stat.S_IFREG | 0o444) << 16
                output.writestr(info, raw)
                files[name] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
            put("__main__.py", ENTRY)
            for member in sorted(members, key=lambda item:item.filename):
                if member.filename.startswith("demiflow/"):
                    with archive.open(member) as source:
                        raw = source.read(MAX_FILE+1)
                    if len(raw) != member.file_size:
                        raise ValueError("Wheel member length differs")
                    put(member.filename, raw)
    if destination.stat().st_size > MAX_TOTAL:
        raise ValueError("Helper archive exceeds release byte budget")
    after = wheel.stat()
    check = hashlib.sha256()
    with wheel.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024), b""):
            check.update(chunk)
    if ((before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
            or check.digest()!=wheel_hash.digest()):
        raise ValueError("Explicit wheel input changed during helper build")
    destination.chmod(0o444)
    digest = hashlib.sha256()
    with destination.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024), b""):
            digest.update(chunk)
    return dict(format="demiflow_analysis_helper_build_v1", wheel_sha256=wheel_hash.hexdigest(), helper_sha256=digest.hexdigest(),
        helper_bytes=destination.stat().st_size, files=files, input_files=count, input_bytes=total,
        execution_tested=False, worker_image_compatibility_tested=False, activation_allowed=False)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--wheel",required=True);parser.add_argument("--output",required=True)
    args=parser.parse_args();result=build(args.wheel,args.output)
    manifest=Path(args.output).with_suffix(".build.json")
    if manifest.exists():
        raise ValueError("Helper build receipt already exists")
    manifest.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({key:value for key,value in result.items() if key!="files"}))


if __name__=="__main__":
    main()
