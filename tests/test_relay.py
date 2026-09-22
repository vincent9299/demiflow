"""relay/cosio/pan123 平台件单测（离线 fixture，签名真算不触网）。"""
import json
import os
import sqlite3
import tarfile
import time

import pytest

from demiflow.collect.cosio import COSCreds, COSIO
from demiflow.collect.relay import (ConsumeLedger, Gates, Heartbeat,
                                    PushLedger, SealSpec, TarPacker,
                                    recover_pairs)


# ---- 账本：schema 与 2026-09-20 部署版逐列兼容 ----

def test_push_ledger_schema_compat(tmp_path):
    led = PushLedger(str(tmp_path))
    cols = [r[1] for r in led.con.execute("PRAGMA table_info(objects)")]
    assert cols == ["key", "etag", "size", "state", "unit", "ts"]
    assert led.seq() == 1 and led.seq() == 2
    led.record_unit([("k1", "e1", 10, "pushed", "u1")])
    assert led.get("k1") == ("e1", 10, "pushed")


def test_consume_ledger_schema_compat(tmp_path):
    led = ConsumeLedger(str(tmp_path))
    cols = [r[1] for r in led.con.execute("PRAGMA table_info(units)")]
    assert cols == ["rel", "size", "md5", "pan_file_id", "state", "ts"]
    led.set("a.tar", 100, "m", 5, "done")
    assert led.get("a.tar") == ("done", "m")
    assert led.counts() == {"done": 1}


# ---- 现役 ledger.db 可被新账本打开（断链可续的关键） ----

def test_ledger_opens_deployed_db(tmp_path):
    db = tmp_path / "ledger.db"
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE objects(key TEXT PRIMARY KEY, etag TEXT, "
                "size INTEGER, state TEXT, unit TEXT, ts REAL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    con.execute("INSERT INTO objects VALUES('old','e',1,'pushed','u',1.0)")
    con.execute("INSERT INTO meta VALUES('seq','7')")
    con.commit()
    con.close()
    led = PushLedger(str(tmp_path))
    assert led.get("old") == ("e", 1, "pushed")
    assert led.seq() == 8                       # 接续部署版序号, 卷名不撞


# ---- 封卷器 ----

class _NullLedger:
    def __init__(self):
        self.n = 0

    def seq(self):
        self.n += 1
        return self.n


def test_tar_packer_seal_and_manifest(tmp_path):
    sealed = []
    blobs_mark = "datasets/demiwtg/kb/blobs/"
    strip = lambda key: key.split(blobs_mark, 1)[1] if blobs_mark in key else key
    packer = TarPacker(str(tmp_path), _NullLedger(), "host", "pre/fix",
                       on_seal=lambda *a: sealed.append(a),
                       spec=SealSpec(cap_bytes=4096, member_name=strip))
    packer.add("lhcos-data/demiwtg-data/datasets/demiwtg/kb/blobs/ab/cd/x.jpg",
               "etag1", b"hello")
    packer.add("lhcos-data/demiwtg-data/datasets/demiwtg/kb/blobs/ef/gh/y.jpg",
               "etag2", b"world!")
    packer.seal()
    assert len(sealed) == 1
    tar_path, man_path, unit_rel, lines = sealed[0]
    assert unit_rel.startswith("pre/fix/") and unit_rel.endswith(".tar")
    with tarfile.open(tar_path) as tf:
        names = tf.getnames()
        assert "ab/cd/x.jpg" in names and "ef/gh/y.jpg" in names
    mans = [json.loads(x) for x in lines]
    assert mans[0]["md5"] == "5d41402abc4b2a76b9719d911017c592"


def test_tar_packer_cap_autoseal(tmp_path):
    sealed = []
    packer = TarPacker(str(tmp_path), _NullLedger(), "host", "p",
                       on_seal=lambda *a: sealed.append(a),
                       spec=SealSpec(cap_bytes=32))
    packer.add("k1", "e", b"x" * 40)            # 超上限立即封卷
    assert len(sealed) == 1
    assert packer.tar is None


def test_recover_pairs_and_tmp_cleanup(tmp_path):
    d = tmp_path / "blobs" / "20260921"
    d.mkdir(parents=True)
    (d / "h-part-000001.tar").write_bytes(b"T")
    (d / "h-part-000001.manifest.jsonl").write_text("{}")
    (d / "h-part-000002.tar").write_bytes(b"T")          # 孤本 tar: 不推不删
    (d / "h-part-000003.tar.tmp").write_bytes(b"half")   # 半卷: 清理
    pairs = recover_pairs(str(tmp_path / "blobs"))
    assert len(pairs) == 1
    assert not (d / "h-part-000003.tar.tmp").exists()
    assert (d / "h-part-000002.tar").exists()


# ---- 闸门 ----

def test_gates_backlog_and_stale(tmp_path):
    (tmp_path / "f").write_bytes(b"x")
    st = {"backlog_bytes": 1, "ts": time.time()}
    g = Gates(str(tmp_path), 1 << 30, status_reader=lambda: st,
              backlog_max=1 << 30)
    assert g.downstream_ok()
    st["backlog_bytes"] = 2 << 30
    assert not g.downstream_ok()               # 积压过线停推
    st["backlog_bytes"], st["ts"] = 0, time.time() - 100000
    g2 = Gates(str(tmp_path), 1 << 30, status_reader=lambda: st,
               stale_max=24 * 3600)
    assert not g2.downstream_ok()              # 心跳超龄停推


def test_gates_reader_error_not_blocking(tmp_path):
    def boom():
        raise RuntimeError("net")
    g = Gates(str(tmp_path), 1 << 30, status_reader=boom)
    assert g.downstream_ok()                   # 自身网络抖动不停推


def test_gates_disk_budget(tmp_path):
    (tmp_path / "f").write_bytes(b"x" * 10)
    g = Gates(str(tmp_path), budget_bytes=8)
    assert not g.disk_ok()                     # 已写 10 字节超预算 8


# ---- 心跳 ----

def test_heartbeat_roundtrip():
    store = {}

    def put(k, d):
        store[k] = json.dumps(d)

    def get(k):
        return json.loads(store[k]) if k in store else None

    Heartbeat(put, "s/x.json").emit(123.0)
    d = Heartbeat.reader(get, "s/x.json")
    assert d["backlog_bytes"] == 123.0 and "host" in d


# ---- cosio.put_smart 路由与 list_entries 解析 ----

class _FakeTransport:
    def __init__(self):
        self.calls = []

    def __call__(self, method, url, headers, data, timeout):
        self.calls.append((method, url, data if not hasattr(data, "read")
                           else "<stream>"))
        if method == "POST" and "uploads=" in url:
            return 200, {}, b"<UploadId>uid1</UploadId>"
        if method == "PUT":
            return 200, {"ETag": '"deadbeef"'}, b""
        if method == "HEAD":
            return 200, {"Content-Length": str(len(self._last_body))}, b""
        if method == "GET":
            self._last_body = self._xml
            return 200, {}, self._xml
        if method == "POST":
            return 200, {}, b"<CompleteMultipartUpload></CompleteMultipartUpload>"
        return 200, {}, b""


def _io(tmp_path, fake):
    io = COSIO(COSCreds("sid", "skey"), "bucket.cos.ap-anywhere.myqcloud.com",
               retries=1, sleep=lambda s: None, transport=fake)
    io._last_body = b""
    return io


def test_put_smart_routes_small_to_single(tmp_path):
    fake = _FakeTransport()
    io = _io(tmp_path, fake)
    p = tmp_path / "small.bin"
    p.write_bytes(b"zzz")
    ok = io.put_smart("k", str(p), "5e52c2e4c8b7b1b1b1b1b1b1b1b1b1b1",
                      multipart_th=1024)
    assert not ok                                # ETag 不匹配 md5 → False
    assert any(m == "PUT" and "?" not in u for m, u, _ in fake.calls[:1])


def test_list_entries_parses_key_size_etag(tmp_path):
    fake = _FakeTransport()
    fake._xml = (b"<ListBucketResult><IsTruncated>false</IsTruncated>"
                 b"<Contents><Key>a/b.tar</Key><Size>100</Size>"
                 b"<ETag>&quot;abc&quot;</ETag>"
                 b"<LastModified>2026-09-18T01:02:03Z</LastModified>"
                 b"</Contents></ListBucketResult>")
    io = _io(tmp_path, fake)
    entries = list(io.list_entries("a/"))
    assert entries == [("a/b.tar", 100, "abc", "2026-09-18T01:02:03Z")]


# ---- pan123 超限类型化 ----

def test_pan123_too_large_typed(monkeypatch, tmp_path):
    import io as _io
    from demiflow.collect import pan123 as P
    client = P.Pan123Client(str(tmp_path / "c.json"), str(tmp_path / "t.json"))

    class _Resp:
        def __init__(self, payload):
            self._buf = _io.BytesIO(payload)

        def read(self):
            return self._buf.read()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    err = json.dumps({"code": 1, "message":
                      "单文件大小超出限制，最大：10.00GB，当前：12.71GB"}).encode()
    monkeypatch.setattr(P.urllib.request, "urlopen",
                        lambda req, timeout: _Resp(err))
    with pytest.raises(P.Pan123FileTooLarge):
        client._http("POST", "https://x")


# ---- pan123 慢速重连(会话轮换) ----

def test_pan123_slow_first_slice_triggers_reset(monkeypatch, tmp_path):
    from demiflow.collect import pan123 as P
    client = P.Pan123Client(str(tmp_path / "c.json"), str(tmp_path / "t.json"),
                            min_slice_rate=10 * 1024 * 1024)  # 阈值抬高: 首片必"慢"
    client.slow_check_min = 0                                  # 测试压门槛: 小文件也走判定
    state = {"logins": 0, "puts": 0, "n": 0}

    def fake_authed(method, url, body=None, **kw):
        if url.endswith("/create"):
            state["n"] += 1
            return {"fileID": None, "reuse": False,
                    "preuploadID": f"pre{state['n']}", "sliceSize": 4 << 20}
        if url.endswith("/get_upload_url"):
            return {"presignedURL": "https://slice.example/put"}
        if url.endswith("/upload_complete"):
            return {"completed": True, "fileID": 4242}
        return {}

    def fake_put(url, chunk):
        state["puts"] += 1
        if state["puts"] == 1:
            time.sleep(0.5)               # 首片 4MiB/0.5s = 8MB/s < 10MB/s 阈值

    def fake_login():
        state["logins"] += 1
        client.min_slice_rate = 0          # 重置后"恢复": 阈值放开

    monkeypatch.setattr(client, "_authed", fake_authed)
    monkeypatch.setattr(client, "_put_slice", fake_put)
    monkeypatch.setattr(client, "login", fake_login)
    p = tmp_path / "f.bin"
    p.write_bytes(b"x" * (5 << 20))        # 2 片(4MiB+1MiB)
    r = client.upload_file(str(p), 1)
    assert r["fileID"] == 4242
    assert state["logins"] == 1            # 恰好一次会话轮换
    assert len(client.resets) == 1 and "before_bps" in client.resets[0]
    assert client.resets[0]["after_done"] is True
    # 第一次尝试传了首片(慢), 重置后第二尝试 2 片 → 共 3 次 put
    assert state["puts"] == 3


def test_tar_packer_default_spec_is_usable_and_unshared(tmp_path):
    """R9：省略 spec 的公开用法必须可用，且不同 packer 不共享配置实例。"""
    sealed = []
    p1 = TarPacker(str(tmp_path), _NullLedger(), "host", "p", on_seal=lambda *a: sealed.append(a))
    from demiflow.collect.relay import SealSpec
    assert isinstance(p1.spec, SealSpec)
    p2 = TarPacker(str(tmp_path), _NullLedger(), "host2", "p", on_seal=lambda *a: sealed.append(a))
    assert p1.spec is not p2.spec
    p1.add("p/obj1", "etag-1", b"payload")
    assert p1.size > 0


# ---- 同路径新版本判定(done 但 md5 变)与同名冲突 ----

def test_consume_ledger_get_returns_md5(tmp_path):
    led = ConsumeLedger(str(tmp_path))
    led.set("a", 1, "md5x", None, "done")
    assert led.get("a") == ("done", "md5x")     # (state, md5): 新版本判定依据


def test_pan123_find_by_name(monkeypatch, tmp_path):
    from demiflow.collect import pan123 as P
    c = P.Pan123Client(str(tmp_path / "c"), str(tmp_path / "t"))
    monkeypatch.setattr(c, "list_dir", lambda pid: [
        {"type": 1, "filename": "d", "fileId": 1},
        {"type": 0, "filename": "f.bin", "fileId": 7},
        {"type": 0, "filename": "g.bin", "fileId": 8}])
    assert c.find_by_name(0, "f.bin") == 7
    assert c.find_by_name(0, "nope") is None
    assert c.find_by_name(0, "d") is None        # 目录不算


# ---- 看门狗进度喂狗 + 上传片级进度回调(2026-09-22 边缘速度段) ----

def test_watchdog_feed_defers_fire():
    """稳态喂狗的看门狗不死：存活时长越过死线两倍(未 _exit(3))。"""
    from demiflow.collect.relay import Watchdog
    wd = Watchdog(deadline_s=0.3)
    t0 = time.time()
    with wd:
        while time.time() - t0 < 0.75:      # 2.5 倍死线
            time.sleep(0.1)
            wd.feed(min_gap_s=0.0)          # 测试关掉 60s 节流
    assert time.time() - t0 >= 0.6


def test_watchdog_feed_throttles_within_gap():
    """节流：距布防不足 min_gap_s 的喂狗是 no-op，不重布定时器。"""
    from demiflow.collect.relay import Watchdog
    wd = Watchdog(deadline_s=60)
    wd._armed_at = time.time()
    armed0, timer0 = wd._armed_at, wd._t
    wd.feed()                                # 刚布防就喂 → 忽略
    assert wd._armed_at == armed0 and wd._t is timer0
    wd._armed_at -= 61                       # 假装 61s 前布防
    wd.feed()
    assert wd._armed_at > armed0 and wd._t is not timer0


def test_pan123_upload_reports_slice_progress(monkeypatch, tmp_path):
    """每片成功 PUT 后回调 on_progress(rate)；秒传路径不回调。"""
    from demiflow.collect import pan123 as P
    client = P.Pan123Client(str(tmp_path / "c"), str(tmp_path / "t"))
    client.slow_check_min = 0

    def fake_authed(method, url, body=None, **kw):
        if url.endswith("/create"):
            return {"reuse": False, "preuploadID": "p1", "sliceSize": 1 << 20}
        if url.endswith("/get_upload_url"):
            return {"presignedURL": "https://slice.example/put"}
        if url.endswith("/upload_complete"):
            return {"completed": True, "fileID": 7}
        return {}

    monkeypatch.setattr(client, "_authed", fake_authed)
    monkeypatch.setattr(client, "_put_slice", lambda u, c: None)
    p = tmp_path / "f.bin"
    p.write_bytes(b"x" * int(2.5 * (1 << 20)))   # 1MiB 片 × 3
    seen = []
    r = client.upload_file(str(p), 1, on_progress=seen.append)
    assert r["fileID"] == 7 and len(seen) == 3
    assert all(v > 0 for v in seen)


def test_pan123_upload_progress_callback_error_is_swallowed(monkeypatch, tmp_path):
    """进度回调抛错绝不影响上传主流程。"""
    from demiflow.collect import pan123 as P
    client = P.Pan123Client(str(tmp_path / "c"), str(tmp_path / "t"))
    client.slow_check_min = 0

    def fake_authed(method, url, body=None, **kw):
        if url.endswith("/create"):
            return {"reuse": False, "preuploadID": "p1", "sliceSize": 1 << 20}
        if url.endswith("/get_upload_url"):
            return {"presignedURL": "https://slice.example/put"}
        if url.endswith("/upload_complete"):
            return {"completed": True, "fileID": 9}
        return {}

    monkeypatch.setattr(client, "_authed", fake_authed)
    monkeypatch.setattr(client, "_put_slice", lambda u, c: None)
    p = tmp_path / "g.bin"
    p.write_bytes(b"y" * (1 << 20))

    def boom(rate):
        raise RuntimeError("callback bug")

    r = client.upload_file(str(p), 1, on_progress=boom)
    assert r["fileID"] == 9


# ---- cosio 分片进度回调(sg 推卷喂狗, 2026-09-22) ----

def test_cosio_put_multipart_reports_part_progress(monkeypatch, tmp_path):
    """每片成功 PUT 后回调 on_progress(rate)；回调抛错吞掉不影响主流程。"""
    from demiflow.collect import cosio as K
    io = K.COSIO.__new__(K.COSIO)          # 跳过 __init__(不发网)
    calls = {"n": 0}

    def fake_request(method, path, params, data, timeout):
        calls["n"] += 1
        if "uploads" in params:            # initiate
            return 200, {}, b"<UploadId>uid1</UploadId>"
        if method == "PUT":                # 分片
            return 200, {"ETag": "e1"}, b""
        if method == "POST" and "uploadId" in params:   # complete
            return 200, {}, b""
        return 200, {}, b""

    monkeypatch.setattr(io, "_request", fake_request)
    monkeypatch.setattr(io, "head", lambda k: None)     # HEAD 核验失败路径也测
    p = tmp_path / "big.bin"
    p.write_bytes(b"z" * (1024 * 1024))                # 2 片(part_size=512KB)
    seen = []

    def cb(rate):
        seen.append(rate)
        if len(seen) == 1:
            raise RuntimeError("callback bug")         # 首片回调炸: 不影响续传

    ok = io.put_multipart("k", str(p), part_size=512 * 1024, on_progress=cb)
    assert ok is False                    # head()!=total → False, 但片已全过
    assert len(seen) == 2 and all(r > 0 for r in seen)
    # 复验核验通过路径
    monkeypatch.setattr(io, "head", lambda k: p.stat().st_size)
    assert io.put_multipart("k", str(p), part_size=512 * 1024,
                            on_progress=lambda r: None) is True
