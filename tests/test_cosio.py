"""Retained COS signing and object primitives; task control uses native v2."""
import json
import urllib.parse
from demiflow.collect.cosio import COSCreds,COSIO,build_host


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



def test_build_host_env_override(monkeypatch):
    monkeypatch.setenv("COS_HOST", "custom.endpoint")
    assert build_host("b", "ap-x") == "custom.endpoint"
    monkeypatch.delenv("COS_HOST")
    assert build_host("b", "ap-x") == "b.cos.ap-x.myqcloud.com"
