"""Authorized bounded media primitives for the dedicated media job.

No URL/path supplied by a notebook or HTTP request is opened here. A trusted
resolver supplies an already authorized read-only file descriptor or bytes.
Codec parsing/decoding belongs in the admitted media worker under its selected
backend and fixed job limits. trusted_local reports managed-process cleanup and
sampled resource observation; these helpers do not provide OS isolation.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import fcntl
import io
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import threading
import time
import wave

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.outputs import asset
from demiflow.forge.schema import ForgeError

MIB = 1024*1024
MAX_RANGE = MIB
LIMITS = {"image": 8*MIB, "audio": 16*MIB, "video": 64*MIB}
PIXELS = 4*1024*1024
CODECS = {"audio": {"pcm_s16le", "pcm_s24le", "pcm_u8", "mp3", "vorbis", "opus", "aac"},
          "video": {"h264", "vp8", "vp9", "theora"}}


class MediaError(ForgeError):
    def __init__(self, message, *, reason):
        super().__init__(message, code="invalid_request" if reason == "range_not_satisfiable" else "operation_rejected")
        self.reason = reason


def unavailable(message):
    raise MediaError(message, reason="media_unavailable")


def _bad_range(message):
    raise MediaError(message, reason="range_not_satisfiable")


def byte_range(header, size):
    """One RFC byte range. Open ranges return a declared <=1MiB subrange."""
    c.integer(size, "media total bytes", 64*MIB, 1)
    if header is None:
        if size > MAX_RANGE:
            _bad_range("Large media requires an explicit range of at most 1MiB")
        return 0, size - 1, 200
    if type(header) is not str or len(header) > 96 or not header.startswith("bytes="):
        _bad_range("A single bounded byte range is required")
    parts = header[6:].split("-")
    if len(parts) != 2 or not any(parts) or any(part and (not part.isascii() or not part.isdecimal() or len(part) > 19) for part in parts):
        _bad_range("Invalid byte range")
    left, right = parts
    if left:
        start = int(left)
        end = int(right) if right else min(size - 1, start + MAX_RANGE - 1)
        end = min(end, size - 1)
    else:
        amount = int(right)
        if amount < 1:
            _bad_range("Empty suffix range")
        start, end = max(0, size - amount), size - 1
    if not 0 <= start <= end < size or end - start + 1 > MAX_RANGE:
        _bad_range("Range is unsatisfiable or exceeds 1MiB")
    return start, end, 206


class MediaBudget:
    """Per helper owner, not per request. Deployment must use one instance."""
    def __init__(self, *, encoded_bytes=32*MIB):
        self.maximum = c.integer(encoded_bytes, "media working bytes", 64*MIB, MAX_RANGE)
        self._lock = threading.Lock()
        self.used = 0

    @contextmanager
    def admit(self, amount):
        c.integer(amount, "media admission bytes", self.maximum, 1)
        if not self._lock.acquire(blocking=False):
            c.reject("Media worker is busy", code="busy")
        try:
            self.used = amount
            yield
        finally:
            self.used = 0
            self._lock.release()


@dataclass(frozen=True)
class MediaRecord:
    """Trusted registry result. It is NEVER deserialized from a public request."""
    project: str
    binding: str
    descriptor: dict
    open_readonly: object


class AuthorizedMedia:
    """The registry must resolve fixed artifact OR table/row/field identities.

    Every request checks access before/after IO and re-resolves the registry;
    no opaque ID, cached descriptor or ETag is an authorization capability.
    """
    def __init__(self, resolve, *, budget=None):
        if not callable(resolve):
            c.reject("Media requires a trusted registry resolver")
        self.resolve = resolve
        self.budget = budget or MediaBudget()

    def _record(self, access, reference, expected_binding, kind):
        access.check()
        c.sha(expected_binding, "media source binding")
        asset(reference, kind)
        record = self.resolve(access, reference, expected_binding, kind)
        if (not isinstance(record, MediaRecord) or record.project != access.project
                or record.binding != expected_binding or record.descriptor != reference):
            c.reject("Media reference differs from its authorized fixed source", code="conflict")
        verified_metadata(reference, kind)
        access.check()
        return record

    def inspect(self, access, reference, *, expected_binding, kind):
        with self.budget.admit(1):
            record = self._record(access, reference, expected_binding, kind)
            return dict(descriptor=record.descriptor, binding=record.binding, available=True)

    def read(self, access, reference, *, expected_binding, kind, range_header=None):
        access.check()
        asset(reference, kind)
        start, end, status = byte_range(range_header, reference["bytes"])
        with self.budget.admit(end - start + 1):
            record = self._record(access, reference, expected_binding, kind)
            descriptor = record.open_readonly()
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_size != reference["bytes"] or fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
                    c.reject("Authorized media changed size/type", code="conflict")
                # Resolver verifies immutable content digest when admitting its
                # fd; range reads must not rehash the entire movie each time.
                value = os.pread(descriptor, end - start + 1, start)
                after = os.fstat(descriptor)
                if len(value) != end - start + 1 or (info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    c.reject("Authorized media changed during range read", code="conflict")
                self._record(access, reference, expected_binding, kind)
                headers={
                    "Content-Type": reference["mime"], "Content-Length": str(len(value)),
                    "Content-Range": f"bytes {start}-{end}/{reference['bytes']}",
                    "Accept-Ranges": "bytes", "ETag": '"'+reference["sha256"]+'"',
                    "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff",
                    "Content-Security-Policy": "default-src 'none'; sandbox"}
                if status != 206:
                    headers.pop("Content-Range")
                return dict(status=status, data=value, headers=headers)
            finally:
                os.close(descriptor)


def verified_metadata(reference, kind):
    asset(reference, kind)
    required = {"width", "height"} if kind == "image" else {"codec", "duration_s"}
    if kind == "video":
        required |= {"width", "height", "fps"}
    if not required <= reference.keys():
        unavailable("Verified media dimensions/duration/codec metadata is missing")
    if kind != "image" and reference["codec"] not in CODECS[kind]:
        unavailable("Media codec is not in the reviewed decoder set")
    return reference


def probe_image(data, *, artifact_id, mime, output_index=None):
    """Real Pillow header, pixel, frame and decoder verification, no full tree."""
    if type(data) is not bytes or not 1 <= len(data) <= LIMITS["image"]:
        c.reject("Image encoded byte limit exceeded", code="resource_limit")
    try:
        from PIL import Image
        from PIL import UnidentifiedImageError
    except ImportError:
        unavailable("Reviewed Pillow decoder is unavailable")
    expected = {"image/png": "PNG", "image/jpeg": "JPEG", "image/gif": "GIF", "image/webp": "WEBP"}
    if mime not in expected:
        unavailable("Image MIME is unsupported; SVG is not executable preview content")
    try:
        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size
            if image.format != expected[mime]:
                c.reject("Image bytes differ from declared MIME", code="conflict")
            if not 0 < width <= 8192 or not 0 < height <= 8192 or width*height > PIXELS:
                c.reject("Image decoded pixel limit exceeded", code="resource_limit")
            # No unbounded animation decode/cache. Static GIF/WebP is supported.
            if getattr(image, "is_animated", False):
                unavailable("Animated images require a reviewed bounded animation decoder")
            image.load()
    except (UnidentifiedImageError, OSError, SyntaxError, Image.DecompressionBombError):
        unavailable("Image decoder rejected invalid or oversized bytes")
    result = dict(artifact_id=artifact_id, sha256=hashlib.sha256(data).hexdigest(),
                  mime=mime, bytes=len(data), width=width, height=height)
    if output_index is not None:
        result["output_index"] = output_index
    return verified_metadata(result, "image")


def probe_wav(data, *, artifact_id):
    if type(data) is not bytes or not 1 <= len(data) <= LIMITS["audio"]:
        c.reject("Audio encoded byte limit exceeded", code="resource_limit")
    try:
        with wave.open(io.BytesIO(data), "rb") as stream:
            channels, width, rate, frames = stream.getnchannels(), stream.getsampwidth(), stream.getframerate(), stream.getnframes()
            if channels not in {1, 2} or width not in {1, 2, 3} or not 1 <= rate <= 192000:
                unavailable("Unsupported PCM audio layout")
            duration = frames/rate
            if duration > 600:
                c.reject("Audio duration exceeds 600s", code="resource_limit")
            # Check declared frames exist without allocating all decoded audio.
            actual = 0
            while True:
                chunk = stream.readframes(16384)
                if not chunk:
                    break
                actual += len(chunk)
            if actual != frames*channels*width:
                unavailable("Truncated WAV frames")
    except (wave.Error, EOFError, OSError):
        unavailable("WAV header/frames are invalid")
    return verified_metadata(dict(artifact_id=artifact_id, sha256=hashlib.sha256(data).hexdigest(), mime="audio/wav",
        bytes=len(data), codec={1: "pcm_u8", 2: "pcm_s16le", 3: "pcm_s24le"}[width], duration_s=duration), "audio")


@dataclass(frozen=True)
class ProbeTool:
    """Pinned admitted tool; path/hash/version come from the deployment lock."""
    path: str
    sha256: str
    version: str

    def verify(self):
        c.sha(self.sha256); c.text(self.version, "probe tool version", 128)
        if not Path(self.path).is_absolute():
            unavailable("Probe tool path must be fixed by the private image")
        from demiflow.forge.io import no_symlinks
        path = no_symlinks(Path(self.path))
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except OSError:
            unavailable("Pinned ffprobe is unavailable")
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 64*MIB or info.st_mode & 0o022:
                unavailable("Pinned ffprobe file metadata differs")
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(MIB), b""):
                digest.update(chunk)
            if digest.hexdigest() != self.sha256:
                unavailable("Pinned ffprobe digest differs from the deployment lock")
        return self.path


def probe_av_fd(fd, *, artifact_id, mime, kind, tool=None):
    """Bounded ffprobe metadata, with explicit missing-tool/codec failures.

    Run only in an admitted media job. Its selected backend owns the process
    lifecycle and reports its capabilities. HTTP never starts this subprocess;
    a fixed ProbeTool is required in every mode. The fd is an immutable asset.
    """
    if kind not in {"audio", "video"}:
        c.reject("AV probe needs an audio/video kind")
    if not isinstance(tool, ProbeTool):
        unavailable("Reviewed ffprobe/codec tool is unavailable")
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= LIMITS[kind]:
        c.reject("Media encoded byte limit exceeded", code="resource_limit")
    argv = [tool.verify(), "-v", "error", "-threads", "1", "-protocol_whitelist", "file,pipe", "-probesize", str(MIB),
            "-analyzeduration", "1000000", "-show_entries",
            "format=duration,format_name:stream=codec_type,codec_name,width,height,avg_frame_rate,duration",
            "-of", "json", f"/proc/self/fd/{fd}"]
    process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        pass_fds=(fd,), start_new_session=True, env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "HOME": "/nonexistent"})
    buffer = bytearray()
    selector = selectors.DefaultSelector()
    try:
        selector.register(process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 5
        while selector.get_map():
            if time.monotonic() >= deadline:
                unavailable("Media probe exceeded the 5s deadline")
            for key, _ in selector.select(min(.1, max(0, deadline-time.monotonic()))):
                chunk = os.read(key.fileobj.fileno(), 4096)
                if not chunk:
                    selector.unregister(key.fileobj)
                else:
                    if len(buffer) + len(chunk) > 65536:
                        c.reject("Media metadata exceeds 64KiB", code="resource_limit")
                    buffer.extend(chunk)
        if process.wait(timeout=max(.001, deadline-time.monotonic())) != 0:
            unavailable("Media probe rejected invalid or unsupported bytes")
    except subprocess.TimeoutExpired:
        unavailable("Media probe exceeded the 5s deadline")
    finally:
        selector.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=2)
        process.stdout.close()
    value = c.decode(bytes(buffer))
    containers = {"audio/wav": {"wav"}, "audio/mpeg": {"mp3"}, "audio/ogg": {"ogg"},
                  "audio/webm": {"matroska", "webm"}, "video/webm": {"matroska", "webm"},
                  "video/ogg": {"ogg"}, "video/mp4": {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}}
    observed = set(value.get("format", {}).get("format_name", "").split(","))
    if mime not in containers or not containers[mime] & observed:
        unavailable("Media container differs from the declared MIME")
    streams = value.get("streams", [])
    if type(streams) is not list or not 1 <= len(streams) <= 2:
        unavailable("Unsupported media stream count")
    wanted = [stream for stream in streams if stream.get("codec_type") == kind]
    if len(wanted) != 1 or any(stream.get("codec_type") not in {"audio", "video"} for stream in streams):
        unavailable("Unsupported media stream layout")
    stream = wanted[0]
    if any(item.get("codec_name") not in CODECS.get(item.get("codec_type"), set()) for item in streams):
        unavailable("Media codec is not in the reviewed decoder set")
    try:
        duration = float(value.get("format", {}).get("duration", stream.get("duration", "nan")))
        result = dict(artifact_id=artifact_id, sha256=_hash_fd(fd, info.st_size), mime=mime,
            bytes=info.st_size, codec=stream["codec_name"], duration_s=duration)
        if kind == "video":
            numerator, denominator = stream["avg_frame_rate"].split("/")
            result.update(width=stream["width"], height=stream["height"], fps=int(numerator)/int(denominator))
    except (ValueError, KeyError, ZeroDivisionError, TypeError):
        unavailable("Media duration/frame metadata is unavailable")
    if not math.isfinite(duration) or duration < 0:
        unavailable("Media duration is unavailable")
    return verified_metadata(result, kind)


def _hash_fd(fd, size):
    digest = hashlib.sha256()
    for offset in range(0, size, MIB):
        chunk = os.pread(fd, min(MIB, size-offset), offset)
        if len(chunk) != min(MIB, size-offset):
            c.reject("Media changed during validation", code="conflict")
        digest.update(chunk)
    return digest.hexdigest()
