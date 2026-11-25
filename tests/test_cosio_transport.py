"""COS client: multipart and listing behaviour (offline fixture, real signing, no network)."""
import json
import os
import time

import pytest

from demiflow.collect.cosio import COSCreds, COSIO


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
