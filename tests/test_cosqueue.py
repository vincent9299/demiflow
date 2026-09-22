"""demiflow COS 队列/IO/执行器测试：假 COS 上全链离线，签名真算。

断言口径全部来自 2026-09-20 第三代实战教训：
- 签名参数三处一致（q-url-param-list 声明 == URL 参数 == FormatString）；
- 瞬态 503/403 退避重试不外泄；
- **成功才 complete**：算子失败 → 释放认领、无 done、批回 todo；
- claim-verify 竞态输家退让且不删他人认领；
- 身份文件值含括号/注释/空行解析健壮。
"""
from __future__ import annotations

import gzip
import io as _io
import json
import urllib.parse

import pytest

from demiflow.collect.cosio import COSCreds, COSIO, build_host
from demiflow.collect.cosqueue import COSQueue
from demiflow.collect import queue_runner


class FakeCOS:
    """内存对象存储，实现 COSIO 注入的 transport 契约。

    on_put(key, body) 钩子供竞态注入（PUT 后改写认归属）。
    """

    def __init__(self):
        self.store = {}
        self.on_put = None
        self.fail_next = []          # 每项: 状态码次数（如 [503, 503]）
        self.seen = []               # (method, url, headers)

    def transport(self, method, url, headers, data, timeout):
        self.seen.append((method, url, headers))
        if self.fail_next:
            st = self.fail_next.pop(0)
            if st:
                return st, {}, b""
        u = urllib.parse.urlsplit(url)
        key = urllib.parse.unquote(u.path.lstrip("/"))
        qs = urllib.parse.parse_qs(u.query)
        if method == "GET" and qs.get("prefix"):
            return self._list(qs)
        if method == "PUT":
            inm = (headers or {}).get("If-None-Match")
            if inm == "*" and key in self.store:
                return 412, {}, b""                 # 条件创建失败：对象已存在
            if self.on_put:
                data = self.on_put(key, data)
            self.store[key] = data or b""
            return 200, {"ETag": '"fake-etag"'}, b""
        if method == "GET":
            return (200, {}, self.store[key]) if key in self.store else (404, {}, b"")
        if method == "HEAD":
            if key in self.store:
                return 200, {"Content-Length": str(len(self.store[key]))}, b""
            return 404, {}, b""
        if method == "DELETE":
            self.store.pop(key, None)
            return 204, {}, b""
        return 405, {}, b""

    def _list(self, qs):
        prefix = qs["prefix"][0]
        marker = qs.get("marker", [""])[0]
        keys = sorted(k for k in self.store if k.startswith(prefix) and k > marker)
        page, truncated = keys[:2], len(keys) > 2      # 每页 2 条，逼出分页路径
        xml = "<ListBucketResult>"
        if truncated:
            xml += f"<NextMarker>{page[-1]}</NextMarker><IsTruncated>true</IsTruncated>"
        for k in page:
            xml += f"<Contents><Key>{k}</Key></Contents>"
        xml += "</ListBucketResult>"
        return 200, {}, xml.encode()


def mkio(fake, **kw):
    io = COSIO(COSCreds("sid", "skey"), "bucket.cos.ap-test.myqcloud.com",
               sleep=lambda s: None, transport=fake.transport, **kw)
    return io


def rows_of(io, key):
    with gzip.GzipFile(fileobj=_io.BytesIO(io.get_bytes(key))) as gz:
        return [json.loads(l) for l in gz.read().decode().splitlines()]


# ---- cosio ----

def test_sign_param_list_consistency():
    """签名声明表 == URL 参数（第三代休眠雷的回归断言）。"""
    fake = FakeCOS()
    io = mkio(fake)
    io.list_prefix("q/batches/")
    for method, url, headers in fake.seen:
        if "prefix=" not in url:
            continue
        auth = headers["authorization"]
        declared = auth.split("q-url-param-list=")[1].split("&")[0]
        params = sorted(urllib.parse.parse_qs(urllib.parse.urlsplit(url).query))
        assert declared == ";".join(params)
        assert params and set(params) <= {"prefix", "max-keys", "marker"}


def test_transient_retry_then_success():
    fake = FakeCOS()
    fake.fail_next = [503, 403]
    io = mkio(fake)
    io.put_bytes("k", b"x")
    assert fake.store["k"] == b"x"


def test_404_is_not_error():
    fake = FakeCOS()
    io = mkio(fake)
    assert io.get_bytes("absent") is None
    assert io.head("absent") is None
    io.delete("absent")                       # 幂等


def test_head_and_etag():
    fake = FakeCOS()
    io = mkio(fake)
    assert io.put_bytes("k", b"hello") == '"fake-etag"'
    assert io.head("k") == 5


# ---- cosqueue 语义 ----

def test_produce_claim_complete_flow():
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    bids = q.produce([{"i": i} for i in range(5)], rows_per_batch=2)
    assert bids == ["b000000", "b000001", "b000002"]
    snap = q.snapshot()
    assert len(snap.batches) == 3 and snap.todo == snap.batches

    h = q.claim("w1")
    assert h.bid == "b000000" and h.worker == "w1"
    snap = q.snapshot()
    assert snap.todo == {"b000001", "b000002"}
    assert rows_of(q.io, h.key) == [{"i": 0}, {"i": 1}]

    q.complete(h)
    assert q.snapshot().done == {"b000000"}


def test_claim_verify_loser_retreats_without_deleting():
    """竞态输家：条件创建 412 → 返回 None，不删他人认领。"""
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    q.produce([{"i": 0}], rows_per_batch=1)
    winner = q.claim("w1")
    assert winner is not None and winner.bid == "b000000"
    assert q.claim("w2") is None
    # 唯一认领对象仍是 w1 的（未被输家删除或覆盖）
    claim_keys = [k for k in fake.store if "/claims/b000000" in k]
    assert claim_keys == ["queue/claims/b000000"]
    assert json.loads(fake.store["queue/claims/b000000"])["worker"] == "w1"


def test_claim_is_exclusive_under_true_race():
    """R2 复现：两个 worker 同 todo 快照交错认领，只有一个成功。"""
    import threading
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    q.produce([{"i": 0}], rows_per_batch=1)
    real_snapshot = q.snapshot
    gated = {"snapshots": 0}
    barrier = threading.Barrier(2)

    def gated_snapshot():
        gated["snapshots"] += 1
        if gated["snapshots"] <= 2:              # 双方都拿到同一 todo 快照
            barrier.wait(timeout=5)
        return real_snapshot()

    q.snapshot = gated_snapshot
    out = []

    def contender(worker):
        out.append(q.claim(worker))

    t1 = threading.Thread(target=contender, args=("w1",))
    t2 = threading.Thread(target=contender, args=("w2",))
    t1.start(); t2.start(); t1.join(); t2.join()
    winners = [h for h in out if h is not None]
    assert len(winners) == 1                     # 恰有一个认领成功
    assert {h.worker for h in winners} <= {"w1", "w2"}


def test_stale_owner_cannot_complete_or_release_new_owner():
    """R2 验收：租约过期被回收后，旧 owner 不得完成/释放新 owner 的批。"""
    from demiflow.collect.cosqueue import _NotClaimOwner
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    q.produce([{"i": 0}], rows_per_batch=1)
    old = q.claim("w1")
    q.requeue_stale(max_age_s=0, now=q._clock() + 1)   # 认领被巡检回收
    fresh = q.claim("w2")                              # 新 owner 接手
    assert fresh is not None and fresh.bid == old.bid
    with pytest.raises(_NotClaimOwner):
        q.complete(old)
    with pytest.raises(_NotClaimOwner):
        q.release(old)
    assert q.snapshot().done == set()                  # 旧 owner 没能写 done
    q.complete(fresh)                                  # 新 owner 正常完成
    assert q.snapshot().done == {"b000000"}


def test_requeue_stale_only_old_unfinished():
    fake = FakeCOS()
    now = [1000.0]                            # 受控时钟：新鲜/超龄全在此定标
    q = COSQueue(mkio(fake), "queue", clock=lambda: now[0])
    q.produce([{"i": i} for i in range(3)], rows_per_batch=1)
    fresh = q.claim("w1")                     # ts=1000（新鲜）
    q.io.call("PUT", "queue/claims/b000002.w2",
              data=json.dumps({"worker": "w2", "ts": 1}).encode())
    done_h = q.claim("w3")
    q.complete(done_h)

    now[0] = 1030.0                           # 假设 30s 后巡检
    reclaimed = q.requeue_stale(max_age_s=60)
    assert reclaimed == ["b000002"]           # 只回收超龄未完成
    snap = q.snapshot()
    assert snap.todo == {"b000002"}
    assert fresh.bid in snap.claimed          # 新鲜认领不动


def test_fetch_batch_tolerates_bad_lines():
    fake = FakeCOS()
    io = mkio(fake)
    raw = b'{"i": 1}\nnot-json\n\n{"i": 2}\n'
    buf = _io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    io.put_bytes("queue/batches/b000000.jsonl.gz", buf.getvalue())
    q = COSQueue(io, "queue")
    rows = q.fetch_batch(type("H", (), {"bid": "b000000",
                                        "key": "queue/batches/b000000.jsonl.gz"})())
    assert rows == [{"i": 1}, {"i": 2}]


# ---- queue_runner：成功才 complete ----

def test_runner_failure_releases_and_retries():
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    q.produce([{"i": 0}], rows_per_batch=1)

    calls = {"n": 0}

    def flaky(rows, ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("dependency missing")

    logs = []
    out = queue_runner.run(flaky, queue=q, worker="w8", workdir="/tmp/qr-test",
                           on_failure_sleep=0, log=logs.append,
                           sleep=lambda s: None)
    assert out == "drained"
    assert calls["n"] == 2
    # 核心断言：失败那次绝无 done；最终成功恰好一条 done
    dones = [k for k in fake.store if "/done/" in k]
    assert len(dones) == 1 and "rc" not in json.loads(fake.store[dones[0]]) or \
        json.loads(fake.store[dones[0]])["rc"] == 0
    assert any("失败" in l for l in logs)
    assert q.snapshot().done == {"b000000"}


def test_runner_drains_and_identity_passthrough():
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")
    q.produce([{"i": 0, "blob": "b0"}], rows_per_batch=1)

    ident = "/tmp/qr-ident-test"
    with open(ident, "w") as f:
        f.write("# comment\n\nKBP_PROXY=socks5://u:p@1.2.3.4:1080\n"
                "KBP_UA=scanner/1.0 (archival; https://x.example)\n")
    seen = {}

    def op(rows, ctx):
        seen.update(ctx.identity)
        for r in rows:
            ctx.io.put_bytes(f"blobs/{r['blob']}", b"IMG")

    out = queue_runner.run(op, queue=q, worker="w9", workdir="/tmp/qr-test",
                           identity_file=ident, log=lambda *a: None)
    assert out == "drained"
    assert seen["KBP_UA"].startswith("scanner/1.0 (")   # 括号原样
    assert fake.store["blobs/b0"] == b"IMG"


def test_runner_idle_poll_executes_batch_arriving_during_wait():
    """R8 复现：idle 等待期间到达的新批必须被第二次认领并执行，不得丢弃。"""
    fake = FakeCOS()
    q = COSQueue(mkio(fake), "queue")

    slept = {"n": 0}

    def fake_sleep(seconds):
        slept["n"] += 1
        if slept["n"] == 1:                     # 第一次 idle 等待期间投放新批
            q.produce([{"i": 0, "blob": "late"}], rows_per_batch=1)

    executed = []

    def op(rows, ctx):
        executed.extend(r["blob"] for r in rows)

    out = queue_runner.run(op, queue=q, worker="w8", workdir="/tmp/qr-test",
                           idle_poll=0.01, log=lambda *a: None,
                           sleep=fake_sleep)
    assert out == "drained"
    assert executed == ["late"]                 # 已认领的批被执行，而非丢失
    assert q.snapshot().done == {"b000000"}
    assert not any(".attempt" in k or "claims/" in k and k.endswith(".w8")
                   for k in fake.store)


def test_load_identity_edge_cases(tmp_path):
    p = tmp_path / "env"
    p.write_text("A=1\nB=x=y\n# c\n\nC=(parens; ok)\nno-equal-line\n")
    got = queue_runner.load_identity(str(p))
    assert got == {"A": "1", "B": "x=y", "C": "(parens; ok)"}


def test_build_host_env_override(monkeypatch):
    monkeypatch.setenv("COS_HOST", "custom.endpoint")
    assert build_host("b", "ap-x") == "custom.endpoint"
    monkeypatch.delenv("COS_HOST")
    assert build_host("b", "ap-x") == "b.cos.ap-x.myqcloud.com"
