"""Optional bounded performance sidecar. Never authorizes or finalizes a run."""

import json
from contextlib import closing
import math
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

MAX_BYTES = 16 * 1024**2
RAW_POINTS = 900
MINUTES = 10080


def _number(value, *, nullable=False, integer=False, minimum=0):
    if value is None and nullable:
        return
    types = (int,) if integer else (int, float)
    if (
        type(value) not in types
        or not math.isfinite(value)
        or not minimum <= value < 2**63
    ):
        raise ValueError("Invalid optional metric number")


def _document(raw, *, minute=False):
    """Validate optional bytes independently of reliable journal failures."""
    if type(raw) is not str or len(raw.encode()) > (1024 if minute else 2048):
        raise ValueError("Optional metric document exceeds budget")
    value = json.loads(raw)
    if type(value) is not dict:
        raise ValueError("Invalid optional metric object")
    _number(value.get("time"))
    if value.get("coverage") not in {"sampled", "partial"}:
        raise ValueError("Invalid optional metric coverage")
    if minute:
        fields = {
            "time",
            "count",
            "cpu_count",
            "cpu_sum",
            "cpu_max",
            "rss_max",
            "output_max",
            "attempts",
            "coverage",
        }
        if set(value) != fields:
            raise ValueError("Invalid optional metric aggregate fields")
        _number(value["count"], integer=True, minimum=1)
        _number(value["cpu_count"], integer=True)
        _number(value["cpu_sum"])
        if value["cpu_count"] > value["count"]:
            raise ValueError("Invalid optional metric aggregate count")
        for key in ("cpu_max", "rss_max", "output_max"):
            _number(value[key], nullable=True, integer=key != "cpu_max")
        attempts = value["attempts"]
        if (
            type(attempts) is not list
            or len(attempts) != 1
            or type(attempts[0]) is not int
            or not 1 <= attempts[0] <= 32
        ):
            raise ValueError("Invalid optional metric aggregate attempt")
        return value
    fields = {
        "time",
        "attempt",
        "epoch",
        "sequence",
        "cpu_cores",
        "rss_bytes",
        "output_bytes",
        "process_count",
        "io",
        "scope",
        "coverage",
        "reasons",
        "usage_complete",
        "throughput",
        "probe_ms",
    }
    if (
        set(value) - fields
        or type(value.get("attempt")) is not int
        or not 1 <= value["attempt"] <= 32
    ):
        raise ValueError("Invalid optional metric fields/attempt")
    if type(value.get("epoch")) is not str or not re.fullmatch(
        r"[0-9a-f]{32}", value["epoch"]
    ):
        raise ValueError("Invalid optional metric epoch")
    _number(value.get("sequence"), integer=True, minimum=1)
    for key in ("cpu_cores", "rss_bytes", "output_bytes", "process_count", "probe_ms"):
        _number(
            value.get(key),
            nullable=True,
            integer=key in {"rss_bytes", "output_bytes", "process_count"},
        )
    if "usage_complete" in value and type(value["usage_complete"]) is not bool:
        raise ValueError("Invalid optional usage flag")
    if "scope" in value and value["scope"] != "observed_worker_descendants":
        raise ValueError("Invalid optional metric scope")
    reasons = value.get("reasons", [])
    if (
        type(reasons) is not list
        or len(reasons) > 16
        or any(type(x) is not str or len(x) > 1536 for x in reasons)
    ):
        raise ValueError("Invalid optional metric reasons")
    io = value.get("io")
    if io is not None:
        if type(io) is not dict or set(io) != {
            k + "_per_s" for k in ("read_bytes", "write_bytes", "rchar", "wchar")
        }:
            raise ValueError("Invalid optional I/O fields")
        for number in io.values():
            _number(number)
    throughput = value.get("throughput")
    if throughput is not None:
        if type(throughput) is not dict or set(throughput) != {
            "action_id",
            "node_id",
            "occurrence",
            "rows_output",
            "rows_per_s",
            "window_s",
        }:
            raise ValueError("Invalid optional throughput fields")
        for key in ("action_id", "node_id"):
            if type(throughput[key]) is not str or not 1 <= len(throughput[key]) <= 64:
                raise ValueError("Invalid optional throughput identity")
        for key in ("occurrence", "rows_output", "rows_per_s", "window_s"):
            _number(
                throughput[key],
                nullable=key == "rows_per_s",
                integer=key in {"occurrence", "rows_output"},
            )
    return value


def _sample(row):
    sequence, attempt, epoch, stamp, raw = row
    value = _document(raw)
    if (value["sequence"], value["attempt"], value["epoch"], value["time"]) != (
        sequence,
        attempt,
        epoch,
        stamp,
    ):
        raise ValueError("Optional metric identity differs")
    return value


def connect(path, *, write=False):
    if path.exists() and (path.is_symlink() or path.stat().st_size > MAX_BYTES):
        raise ValueError("Telemetry unavailable or over budget")
    db = sqlite3.connect(
        path if write else path.resolve().as_uri() + "?mode=ro",
        uri=not write,
        timeout=0.1,
    )
    db.execute("PRAGMA cache_size=-2048")
    if write:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA max_page_count=4096")
        db.executescript(
            """
        CREATE TABLE IF NOT EXISTS samples(sequence INTEGER PRIMARY KEY,attempt INTEGER,epoch TEXT,time REAL,document TEXT);
        CREATE TABLE IF NOT EXISTS minutes(attempt INTEGER,bucket INTEGER,document TEXT,PRIMARY KEY(attempt,bucket));
        CREATE INDEX IF NOT EXISTS minute_time ON minutes(bucket,attempt);
        """
        )
    return db


class Telemetry:
    def __init__(self, target, attempt):
        self.path = Path(target) / "telemetry.sqlite"
        self.attempt = attempt
        self.epoch = uuid.uuid4().hex
        self.db = connect(self.path, write=True)
        self.sequence = self.db.execute(
            "SELECT coalesce(max(sequence),0) FROM samples"
        ).fetchone()[0]

    def write(self, sample):
        self.sequence += 1
        now = sample["time"]
        sample = dict(
            sample, attempt=self.attempt, epoch=self.epoch, sequence=self.sequence
        )
        raw = json.dumps(sample, separators=(",", ":"), allow_nan=False)
        if len(raw.encode()) > 2048:
            raise ValueError("Optional telemetry sample byte budget exceeded")
        bucket = int(now // 60)
        with self.db:
            self.db.execute(
                "INSERT INTO samples VALUES (?,?,?,?,?)",
                (self.sequence, self.attempt, self.epoch, now, raw),
            )
            self.db.execute(
                "DELETE FROM samples WHERE sequence<=? OR time<?",
                (self.sequence - RAW_POINTS, now - 1800),
            )
            old = self.db.execute(
                "SELECT document FROM minutes WHERE attempt=? AND bucket=?",
                (self.attempt, bucket),
            ).fetchone()
            aggregate = (
                json.loads(old[0])
                if old
                else dict(
                    time=bucket * 60,
                    count=0,
                    cpu_count=0,
                    cpu_sum=0.0,
                    cpu_max=None,
                    rss_max=None,
                    output_max=None,
                    attempts=[],
                    coverage="sampled",
                )
            )
            aggregate["count"] += 1
            if self.attempt not in aggregate["attempts"]:
                aggregate["attempts"].append(self.attempt)
            cpu = sample.get("cpu_cores")
            if cpu is not None:
                aggregate["cpu_count"] += 1
                aggregate["cpu_sum"] += cpu
                aggregate["cpu_max"] = max(aggregate["cpu_max"] or 0, cpu)
            for key, source in [
                ("rss_max", "rss_bytes"),
                ("output_max", "output_bytes"),
            ]:
                if sample.get(source) is not None:
                    aggregate[key] = max(aggregate[key] or 0, sample[source])
            if sample.get("coverage") != "sampled":
                aggregate["coverage"] = "partial"
            encoded = json.dumps(aggregate, separators=(",", ":"), allow_nan=False)
            if len(encoded.encode()) > 1024:
                raise ValueError("Telemetry aggregate byte budget exceeded")
            self.db.execute(
                "INSERT OR REPLACE INTO minutes VALUES (?,?,?)",
                (self.attempt, bucket, encoded),
            )
            self.db.execute(
                "DELETE FROM minutes WHERE bucket<?", (bucket - MINUTES + 1,)
            )
            self.db.execute(
                "DELETE FROM minutes WHERE rowid IN (SELECT rowid FROM minutes ORDER BY bucket DESC,attempt DESC LIMIT -1 OFFSET ?)",
                (MINUTES,),
            )
        return sample

    def close(self):
        self.db.close()


def latest(target, attempt=None):
    path = Path(target) / "telemetry.sqlite"
    try:
        if not path.exists():
            return dict(availability="unavailable", sample=None)
        with closing(connect(path)) as db:
            query = "SELECT sequence,attempt,epoch,time,substr(document,1,2049) FROM samples"
            args = ()
            if attempt is not None:
                query += " WHERE attempt=?"
                args = (attempt,)
            row = db.execute(query + " ORDER BY sequence DESC LIMIT 1", args).fetchone()
            if not row:
                return dict(availability="unavailable", sample=None)
            value = _sample(row)
            return dict(
                availability="available",
                sample=value,
                age_s=max(0, time.time() - value["time"]),
            )
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        RecursionError,
        sqlite3.Error,
    ):
        return dict(availability="unavailable", sample=None)


def series(target, *, attempt=None, points=600, resolution="raw"):
    if (
        type(points) is not int
        or not 1 <= points <= 600
        or resolution not in {"raw", "minute"}
    ):
        raise ValueError("Invalid telemetry query budget")
    path = Path(target) / "telemetry.sqlite"
    try:
        if not path.exists():
            return dict(points=[], availability="unavailable")
        with closing(connect(path)) as db:
            if resolution == "raw":
                where = " WHERE attempt=?" if attempt is not None else ""
                args = (attempt, points) if attempt is not None else (points,)
                rows = db.execute(
                    "SELECT sequence,attempt,epoch,time,substr(document,1,2049) FROM samples"
                    + where
                    + " ORDER BY sequence DESC LIMIT ?",
                    args,
                )
            else:
                where = " WHERE attempt=?" if attempt is not None else ""
                args = (attempt, points) if attempt is not None else (points,)
                rows = db.execute(
                    "SELECT attempt,bucket,substr(document,1,1025) FROM minutes"
                    + where
                    + " ORDER BY bucket DESC LIMIT ?",
                    args,
                )
            result = []
            size = 0
            for row in rows:
                if resolution == "raw":
                    value = _sample(row)
                else:
                    value = _document(row[-1], minute=True)
                    if value["attempts"] != [row[0]] or value["time"] != row[1] * 60:
                        raise ValueError("Optional aggregate identity differs")
                size += len(row[-1].encode())
                if size > 240 * 1024:
                    break
                if (
                    resolution == "minute"
                    and attempt is not None
                    and attempt not in value["attempts"]
                ):
                    continue
                result.append(value)
            return dict(
                points=list(reversed(result)),
                availability="available",
                resolution=resolution,
                retention="last_execution_window",
                truncated=size > 240 * 1024,
            )
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        OverflowError,
        RecursionError,
        sqlite3.Error,
    ):
        return dict(points=[], availability="unavailable")


def observe(target, attempt, worker_pid, deadline):
    """One optional process, no overlapping probes and no browser dependency."""
    import resource
    from ..resources.process_observation import ProcessSampler

    # stdlib only: measured virtual footprint plus explicit allocation headroom.
    baseline = int(Path("/proc/self/statm").read_text().split()[0]) * os.sysconf(
        "SC_PAGE_SIZE"
    )
    resource.setrlimit(
        resource.RLIMIT_AS, (baseline + 64 * 1024**2, baseline + 64 * 1024**2)
    )
    resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
    sampler = ProcessSampler()
    store = Telemetry(target, attempt)
    window = []
    try:
        while time.monotonic() < deadline:
            started = time.monotonic()
            sample = sampler.sample(worker_pid)
            sample["time"] = time.time()
            path = Path(target) / "run.sqlite"
            with closing(
                sqlite3.connect(
                    path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1
                )
            ) as db:
                raw = db.execute("SELECT document FROM execution_state").fetchone()[0]
                if len(raw) > 65536:
                    raise ValueError("Control sample exceeds budget")
                state = json.loads(raw)
                sample["output_bytes"] = state.get("usage", {}).get("output_bytes")
                sample["usage_complete"] = state.get("usage_complete", False)
                row = db.execute(
                    "SELECT action_id,node_id,occurrence,json_extract(document,'$.rows_output') FROM node_observations WHERE attempt=? AND action_id=node_id ORDER BY terminal,sequence DESC LIMIT 1",
                    (attempt,),
                ).fetchone()
                if row and row[3] is not None:
                    identity = tuple(row[:3])
                    count = row[3]
                    window = [
                        p for p in window if p[1] == identity and p[0] >= started - 10
                    ][-5:]
                    rate = None
                    if window and count >= window[0][2] and started > window[0][0]:
                        rate = (count - window[0][2]) / (started - window[0][0])
                    window.append((started, identity, count))
                    sample["throughput"] = dict(
                        action_id=row[0],
                        node_id=row[1],
                        occurrence=row[2],
                        rows_output=count,
                        rows_per_s=rate,
                        window_s=started - window[0][0],
                    )
                else:
                    window = []
                    sample["throughput"] = None
            duration = time.monotonic() - started
            sample["probe_ms"] = duration * 1000
            if duration > 0.05:
                sample["coverage"] = "partial"
                sample["reasons"].append("probe_time_budget")
            store.write(sample)
            if state["status"] not in {"starting", "running", "stopping"}:
                break
            time.sleep(max(0.01, 2 - (time.monotonic() - started)))
    finally:
        store.close()
