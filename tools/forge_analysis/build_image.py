#!/usr/bin/env python3
"""Assemble an auditable amd64 worker rootfs from a frozen local interpreter.

This copies only stdlib, selected distribution RECORD files, their ELF runtime
dependencies, and the explicitly supplied DemiFlow wheel. It neither installs
into the shared environment nor starts Python/Jupyter/OCI inside the image.
The result still requires exact release Review and real B1 calibration.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import resource
import shutil
import stat
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import zipfile

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

MAX_BYTES = 2*1024**3
MAX_FILES = 32768
MAX_ONE_FILE = 512*1024**2
ROOTS = ("pyarrow", "datafusion", "pylance", "PyYAML", "click", "packaging", "cloudpickle", "psutil",
         "jupyter_client", "ipykernel", "nbformat", "pandas", "matplotlib", "pillow")
_SKIP_STDLIB = {"site-packages", "__pycache__", "test", "tests", "idlelib", "tkinter", "turtledemo", "ensurepip"}
_SKIP_HOOKS = {"sitecustomize.py", "usercustomize.py", "_virtualenv.py"}


def fail(message):
    raise ValueError(message)


def sha_file(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024), b""):
            value.update(chunk)
    return value.hexdigest()


def relative(value):
    path = PurePosixPath(value)
    if not value or len(value) > 1024 or len(path.parts) > 32 or path.is_absolute() or ".." in path.parts or str(path) != value:
        fail("Unsafe image-relative path")
    return path


def readelf_dynamic(source):
    """Do not capture arbitrary tool output into an unbounded Python buffer."""
    def limits():
        resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 65536))
        resource.setrlimit(resource.RLIMIT_AS, (768*1024**2, 768*1024**2))
        resource.setrlimit(resource.RLIMIT_CPU, (3, 3))
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(["readelf", "-d", str(source)], stdout=output, stderr=subprocess.DEVNULL,
                                stdin=subprocess.DEVNULL, timeout=5, preexec_fn=limits)
        if result.returncode or output.tell() > 65536:
            fail("Cannot inspect bounded ELF dynamic metadata")
        output.seek(0)
        return output.read(65536).decode("utf-8")


def distributions():
    pending = list(ROOTS)
    found = {}
    environment = {**default_environment(), "extra": ""}
    while pending:
        name = canonicalize_name(pending.pop())
        if name in found:
            continue
        if len(found) >= 128:
            fail("Dependency closure exceeds distribution limit")
        dist = metadata.distribution(name)
        record = Path(dist._path)/"RECORD"
        if not record.is_file() or record.stat().st_size > 4*1024**2:
            fail("Installed distribution requires a bounded RECORD: " + name)
        files = dist.files
        if files is None or len(files) > MAX_FILES:
            fail("Installed distribution file list exceeds budget: " + name)
        found[name] = dist
        for requirement in dist.requires or []:
            item = Requirement(requirement)
            if item.url:
                fail("Image dependency must be an installed version, not an external URL")
            if item.marker is None or item.marker.evaluate(environment):
                dependency = metadata.distribution(item.name)
                if not item.specifier.contains(dependency.version, prereleases=True):
                    fail("Installed dependency version does not satisfy " + item.name)
                pending.append(item.name)
    return found


class Builder:
    def __init__(self, destination):
        self.root = destination/"rootfs"
        self.root.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.files = {}
        self.source_destinations = {}
        self.directories = set()
        self.bytes = 0
        self.media = {}

    def _admit(self, name, size):
        relative(name)
        if name in self.files:
            return
        if len(self.files) >= MAX_FILES or size > MAX_ONE_FILE or self.bytes + size > MAX_BYTES:
            fail("Worker image file/byte reservation exhausted")
        parents = {str(parent) for parent in PurePosixPath(name).parents if str(parent) != "."}
        if len(self.directories | parents) > 65536:
            fail("Worker image directory reservation exhausted")
        self.directories.update(parents)

    def add(self, source, name, *, provenance, executable=None):
        source = source.resolve(strict=True)
        info = source.stat()
        if not stat.S_ISREG(info.st_mode):
            fail("Image inputs must be regular files")
        self._admit(name, info.st_size)
        source_sha = sha_file(source)
        if name in self.files:
            if self.files[name]["sha256"] != source_sha:
                fail("Conflicting runtime dependency at " + name)
            return
        destination = self.root/name
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as incoming, destination.open("xb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024*1024)
        after = source.stat()
        if (info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns) or sha_file(destination) != source_sha:
            fail("Image source changed during copy")
        is_executable = bool(info.st_mode & 0o111) if executable is None else executable
        mode = 0o555 if is_executable else 0o444
        destination.chmod(mode)
        self.files[name] = dict(sha256=source_sha, bytes=info.st_size, mode=mode,
                                source=str(source), provenance=provenance)
        self.source_destinations[source] = name
        self.bytes += info.st_size

    def data(self, raw, name, *, provenance):
        self._admit(name, len(raw))
        if name in self.files:
            fail("Duplicate generated image file")
        destination = self.root/name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw); destination.chmod(0o444)
        self.files[name] = dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), mode=0o444,
                                source=None, provenance=provenance)
        self.bytes += len(raw)

    def interpreter(self):
        self.add(Path(sys.executable), "usr/local/bin/python3.11", provenance="fixed CPython interpreter", executable=True)
        base = Path(sysconfig.get_path("stdlib")).resolve()
        entries = 0
        for directory, folders, files in os.walk(base, followlinks=False):
            folders[:] = sorted(name for name in folders if name not in _SKIP_STDLIB)
            if Path(directory) == base:
                folders[:] = [name for name in folders if name in sys.stdlib_module_names or name == "lib-dynload"]
            for name in sorted(files):
                entries += 1
                if entries > MAX_FILES:
                    fail("Standard-library inventory exceeds file limit")
                if name.endswith((".pyc", ".pyo")) or name in _SKIP_HOOKS:
                    continue
                if Path(directory) == base and Path(name).stem not in sys.stdlib_module_names and not name.startswith(("_sysconfigdata_", "LICENSE")):
                    continue
                path = Path(directory)/name
                if path.is_symlink() and not path.resolve().is_relative_to(base):
                    fail("Unexpected external stdlib symlink")
                self.add(path, "usr/local/lib/python3.11/" + path.relative_to(base).as_posix(), provenance="fixed CPython stdlib")

    def packages(self, selected):
        site = Path(sysconfig.get_path("purelib")).resolve()
        for name, dist in sorted(selected.items()):
            for entry in sorted(dist.files, key=str):
                path = Path(dist.locate_file(entry))
                # Console scripts and direct_url/install state are not runtime
                # code. No personal/venv-wide .pth startup hooks are copied.
                if not path.is_relative_to(site) or ".." in path.relative_to(site).parts:
                    continue
                target = path.relative_to(site).as_posix()
                if "__pycache__" in Path(target).parts or target.endswith((".pyc", ".pyo", ".pth", "/direct_url.json", "/RECORD")):
                    continue
                if not path.is_file() or not path.resolve().is_relative_to(site):
                    fail("Package RECORD references missing/outside input: " + name)
                self.add(path, "usr/local/lib/python3.11/site-packages/" + target, provenance=name + "==" + dist.version)

    def wheel(self, wheel):
        wheel_sha = sha_file(wheel)
        with zipfile.ZipFile(wheel) as archive:
            entries = archive.infolist()
            if len(entries) > 4096 or sum(item.file_size for item in entries) > 64*1024**2:
                fail("DemiFlow wheel exceeds package reservation")
            for item in entries:
                path = relative(item.filename)
                if (path.parts[0] != "demiflow" and not re.fullmatch(r"demiflow-[A-Za-z0-9_.+-]+\.dist-info", path.parts[0])) or item.is_dir():
                    fail("Unexpected wheel member")
                if item.file_size > 16*1024**2:
                    fail("DemiFlow wheel member exceeds limit")
                self.data(archive.read(item), "usr/local/lib/python3.11/site-packages/" + item.filename,
                          provenance="review-candidate DemiFlow wheel " + wheel_sha)

    def media_tools(self, lock_path):
        """Explicit reviewed local binaries only; never discover/download tools.

        Tool versions are release metadata, not verified by running an input.
        Actual codec/version behavior belongs to the isolated B1 calibration.
        """
        with Path(lock_path).open("rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
                fail("Media input lock must be a bounded regular file")
            raw = source.read(65537)
        if len(raw) > 65536:
            fail("Media input lock exceeds byte budget")
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {"format", "review_ref", "tools"} or value["format"] != "demiflow_analysis_media_inputs_v1":
            fail("Unsupported reviewed media input lock")
        if not isinstance(value["review_ref"], str) or not 1 <= len(value["review_ref"]) <= 1024:
            fail("Media tools need an explicit release review reference")
        tools = value["tools"]
        if not isinstance(tools, list) or not 1 <= len(tools) <= 2:
            fail("Only the fixed ffprobe/ffmpeg tool set is accepted")
        for item in tools:
            fields = {"name", "version", "path", "sha256", "license_path", "license_sha256", "configuration"}
            if not isinstance(item, dict) or set(item) != fields or item["name"] not in {"ffprobe", "ffmpeg"} or item["name"] in self.media:
                fail("Unknown or duplicate fixed media tool")
            for key in ("version", "configuration"):
                if not isinstance(item[key], str) or not 1 <= len(item[key]) <= 4096:
                    fail("Media version/configuration must be explicit and bounded")
            for path_key, sha_key, maximum in (("path", "sha256", 256*1024**2), ("license_path", "license_sha256", 1024**2)):
                path = Path(item[path_key])
                if not path.is_absolute() or not path.is_file() or path.stat().st_size > maximum:
                    fail("Media input must be an explicit bounded local regular file")
                if not isinstance(item[sha_key], str) or not re.fullmatch('[0-9a-f]{64}', item[sha_key]) or sha_file(path) != item[sha_key]:
                    fail("Reviewed media input digest differs")
            source = Path(item["path"])
            with source.open("rb") as stream:
                if stream.read(4) != b"\x7fELF":
                    fail("Media input must be a reviewed ELF binary, never a shell wrapper")
            target = "usr/local/bin/" + item["name"]
            self.add(source, target, provenance="reviewed media " + item["version"], executable=True)
            self.add(Path(item["license_path"]), "opt/demiflow/notices/" + item["name"] + "-license",
                     provenance="reviewed media license", executable=False)
            # Recheck the copied content against the approved digest as well.
            if (self.files[target]["sha256"] != item["sha256"]
                    or self.files["opt/demiflow/notices/" + item["name"] + "-license"]["sha256"] != item["license_sha256"]):
                fail("Media input changed before copying")
            self.media[item["name"]] = {key:item[key] for key in ("version", "sha256", "configuration", "license_sha256")}
        self.media_review = value["review_ref"]
        self.media_lock_sha256 = hashlib.sha256(raw).hexdigest()

    def elf_dependencies(self):
        pending = []
        for source, target in list(self.source_destinations.items()):
            with source.open("rb") as stream:
                if stream.read(4) == b"\x7fELF":
                    pending.append(source)
        visited = set()
        system = [Path(sys.prefix)/"lib", Path("/lib/x86_64-linux-gnu"), Path("/usr/lib/x86_64-linux-gnu"), Path("/lib64"), Path("/usr/lib64"), Path("/lib"), Path("/usr/lib")]
        while pending:
            source = pending.pop()
            if source in visited:
                continue
            if len(visited) >= 2048:
                fail("ELF dependency inventory exceeds limit")
            visited.add(source)
            body = readelf_dynamic(source)
            needed = re.findall(r"\(NEEDED\).*?\[([^]]+)\]", body)
            runtime_paths = re.findall(r"\((?:RPATH|RUNPATH)\).*?\[([^]]+)\]", body)
            lookup = [source.parent]
            for group in runtime_paths:
                for part in group.split(":"):
                    expanded = part.replace("${ORIGIN}", str(source.parent)).replace("$ORIGIN", str(source.parent))
                    if "$" in expanded:
                        fail("Unsupported ELF runtime search token")
                    lookup.append(Path(expanded))
            for name in needed:
                if "/" in name:
                    fail("ELF dependency names must be sonames")
                dependency = next((path/name for path in lookup + system if (path/name).is_file()), None)
                if dependency is None:
                    fail("Unresolved ELF dependency: " + name)
                dependency = dependency.resolve()
                if dependency not in self.source_destinations:
                    self.add(dependency, "usr/local/lib/" + name, provenance="transitive ELF runtime")
                pending.append(dependency)
        self.add(Path("/lib64/ld-linux-x86-64.so.2"), "lib64/ld-linux-x86-64.so.2", provenance="fixed amd64 ELF loader", executable=True)

    def finish(self, destination, selected, wheel):
        for path in (Path(sys.prefix)/"LICENSE", Path(sys.prefix)/"LICENSE.txt",
                     Path("/usr/share/doc/libc6/copyright"), Path("/usr/share/doc/libgcc-s1/copyright"),
                     Path("/usr/share/doc/libstdc++6/copyright"), Path("/usr/share/doc/zlib1g/copyright"),
                     Path("/usr/share/doc/libssl3/copyright")):
            if path.is_file() and path.stat().st_size <= 1024*1024:
                self.add(path, "opt/demiflow/notices/" + path.parent.name + "-" + path.name, provenance="runtime license notice", executable=False)
        for name in ("proc", "sys/fs/cgroup", "dev", "scratch", "work", "job", "broker", "sources", "objects", "pending", "input", "var/lib/demiflow-analysis/native-slots", "opt/demiflow/kernel"):
            (self.root/name).mkdir(parents=True, exist_ok=True)
        self.data(b"root:x:0:0:DemiFlow sandbox:/work:/usr/local/bin/python3.11\n", "etc/passwd", provenance="fixed sandbox identity")
        self.data(b"root:x:0:\n", "etc/group", provenance="fixed sandbox identity")
        self.data(b"127.0.0.1 localhost\n::1 localhost\n", "etc/hosts", provenance="fixed loopback only")
        for directory, folders, files in os.walk(self.root, topdown=False):
            Path(directory).chmod(0o555)
        runtime = {name: {key: value for key, value in item.items() if key in {"sha256", "bytes", "mode"}} for name, item in sorted(self.files.items())}
        digest = hashlib.sha256(json.dumps(runtime, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        lock = dict(format="demiflow_analysis_image_lock_v1", architecture=platform.machine(), python=sys.version,
                    distributions={name: dist.version for name, dist in sorted(selected.items())}, wheel_sha256=sha_file(wheel),
                    files=dict(sorted(self.files.items())), image_tree_sha256=digest, files_count=len(self.files), bytes=self.bytes,
                    isolation_tested=False, activation_allowed=False,
                    media_tools=self.media, media_input_lock_sha256=getattr(self, "media_lock_sha256", None),
                    media_review_ref=getattr(self, "media_review", None),
                    excluded="personal environment/home/credentials, installed direct_url, console scripts, .pth, caches, stdlib tests/GUI/ensurepip")
        (destination/"image-lock.json").write_text(json.dumps(lock, indent=2, ensure_ascii=False)+"\n")
        # Deterministic archive includes no source filesystem ownership or mtimes.
        with tarfile.open(destination/"worker-rootfs.tar", "w", format=tarfile.PAX_FORMAT) as archive:
            for path in sorted(self.root.rglob("*")):
                name = path.relative_to(self.root).as_posix()
                info = archive.gettarinfo(str(path), arcname=name)
                info.uid = info.gid = info.mtime = 0; info.uname = info.gname = ""
                if path.is_file():
                    with path.open("rb") as source:
                        archive.addfile(info, source)
                else:
                    archive.addfile(info)
        return dict(image_tree_sha256=digest, archive_sha256=sha_file(destination/"worker-rootfs.tar"),
                    lock_sha256=sha_file(destination/"image-lock.json"), files=len(self.files), bytes=self.bytes,
                    activation_allowed=False, isolation_tested=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--media-lock", type=Path, help="Explicit reviewed local ffprobe/ffmpeg inputs; never PATH discovery")
    args = parser.parse_args()
    if platform.machine() != "x86_64" or sys.version_info[:2] != (3, 11):
        fail("This first fixed image requires amd64 CPython3.11")
    selected = distributions()
    if args.plan:
        print(json.dumps(dict(distributions={name: dist.version for name, dist in sorted(selected.items())}, build=False, activation_allowed=False), indent=2))
        return
    if args.wheel is None or args.output is None or args.output.exists() or not args.wheel.is_file():
        fail("Provide an existing reviewed-candidate wheel and a new owned output directory")
    parent = args.output.parent
    while not parent.exists():
        parent = parent.parent
    filesystem = os.statvfs(parent)
    if filesystem.f_bavail * filesystem.f_frsize < 2*MAX_BYTES + 512*1024**2:
        fail("Insufficient free disk for bounded rootfs + archive + metadata reservation")
    args.output.mkdir(mode=0o700, parents=True)
    builder = Builder(args.output)
    builder.interpreter(); builder.packages(selected); builder.wheel(args.wheel)
    if args.media_lock is not None:
        builder.media_tools(args.media_lock)
    builder.elf_dependencies()
    print(json.dumps(builder.finish(args.output, selected, args.wheel), indent=2))


if __name__ == "__main__":
    main()
