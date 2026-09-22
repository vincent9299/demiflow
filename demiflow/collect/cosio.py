"""demiflow COS IO 原语：签名请求/凭证约定/瞬态重试/前缀列举。

自第三代采集队列实战沉淀（2026-09-20，机制归引擎）：
- **签名一致性**：q-url-param-list 必须与 FormatString 第三字段的参数集
  逐一致（手搓变体曾把参数签进串却声明空列表/列表字面量，SignatureDoesNotMatch
  成全 fleet 休眠雷——本模块只出这一种正确拼法）；
- **瞬态重试**：新出口 IP 的突发签名 LIST 会触发 COS 瞬时 403，一击即杀过
  整批 worker——所有操作默认三次退避重试（403/429/5xx/传输错误）；
- 凭证约定：``sid:skey`` 单行文件或环境变量；路径/变量名由消费方注入，
  引擎只给缺省探测顺序。

契约：
- ``COSCreds``：凭证（from_env / from_file / discover 探测约定路径）；
- ``COSIO.call(method, key)``：对象级 GET/PUT/DELETE，统一返回
  ``(status, headers, body)``；404 原样返回不抛（调用方按语义解释）；
- ``COSIO.list_prefix(prefix)``：分页列举（NextMarker 缺失时回退末 key）；
- ``COSIO.get_bytes/put_bytes/head/delete``：字节级原语（put 透传 ETag
  供消费方做内容校验）。

与 store.py 的关系：store 管本地内容寻址落盘；本模块管远端对象存取。
传输层可注入（tests 用假 COS 全链离线验证，签名真算）。
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

__all__ = ["COSCreds", "COSIO", "build_host"]

# 瞬态可重试状态码：403 计入（新 IP 突发限频）但连续 403 会在重试耗尽后上抛
_RETRY_STATUS = frozenset({403, 429, 500, 502, 503, 504})
_DEFAULT_BACKOFF = (5.0, 15.0)


@dataclass(frozen=True)
class COSCreds:
    """对象存储凭证：secret id + key。"""

    sid: str
    skey: str

    @classmethod
    def from_env(cls, sid_var: str = "COS_SECRET_ID",
                 key_var: str = "COS_SECRET_KEY") -> "COSCreds":
        sid, skey = os.environ.get(sid_var, ""), os.environ.get(key_var, "")
        if not sid or not skey:
            raise RuntimeError(f"环境变量 {sid_var}/{key_var} 不完整")
        return cls(sid, skey)

    @classmethod
    def from_file(cls, path: str) -> "COSCreds":
        """``sid:skey`` 单行文件（chmod 600 由部署方负责）。"""
        line = open(path).read().strip()
        sid, _, skey = line.partition(":")
        if not sid or not skey:
            raise RuntimeError(f"凭证文件格式应为 sid:skey：{path}")
        return cls(sid, skey)

    @classmethod
    def discover(cls, paths: tuple[str, ...] = ()) -> "COSCreds":
        """探测顺序：环境变量 → 显式路径 → 约定路径（/tmp 态目录殿后）。"""
        try:
            return cls.from_env()
        except RuntimeError:
            pass
        for p in tuple(paths) + (os.path.expanduser("~/.config/demiflow/cos_creds"), "/tmp/cos_creds"):
            if os.path.exists(p):
                return cls.from_file(p)
        raise RuntimeError("无 COS 凭证：环境变量与约定路径均未命中")


def build_host(bucket: str, region: str, env_var: str = "COS_HOST") -> str:
    """桶域名；环境变量可整体覆盖（endpoint 自定义场景）。"""
    return os.environ.get(env_var) or f"{bucket}.cos.{region}.myqcloud.com"


def _urllib_transport(method: str, url: str, headers: Mapping[str, str],
                      data: Optional[bytes], timeout: float):
    req = urllib.request.Request(url, data=data, method=method, headers=dict(headers))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()


class COSIO:
    """签名 + 重试 + 列举的对象存取面。凭证明文只在内存与请求签名里。"""

    def __init__(self, creds: COSCreds, host: str, *,
                 retries: int = 3, backoff: tuple = _DEFAULT_BACKOFF,
                 sleep: Callable[[float], None] = time.sleep,
                 transport: Callable = _urllib_transport,
                 clock: Callable[[], float] = time.time):
        self.creds, self.host = creds, host
        self.retries, self.backoff = retries, backoff
        self._sleep, self._transport, self._clock = sleep, transport, clock

    # ---- 签名（唯一拼法：参数集三处一致——URL / FormatString / 声明表） ----

    def _sign(self, method: str, path: str, params: dict) -> str:
        sid, skey = self.creds.sid, self.creds.skey
        now = int(self._clock())
        kt = f"{now - 60};{now + 900}"
        sk = hmac.new(skey.encode(), kt.encode(), hashlib.sha1).hexdigest()
        p = "&".join(f"{k.lower()}={urllib.parse.quote(str(v), safe='')}"
                     for k, v in sorted(params.items()))
        hs = f"{method.lower()}\n{path}\n{p}\nhost={self.host}\n"
        sts = f"sha1\n{kt}\n{hashlib.sha1(hs.encode()).hexdigest()}\n"
        sigv = hmac.new(sk.encode(), sts.encode(), hashlib.sha1).hexdigest()
        declared = ";".join(sorted(k.lower() for k in params))
        return (f"q-sign-algorithm=sha1&q-ak={sid}&q-sign-time={kt}&q-key-time={kt}"
                f"&q-header-list=host&q-url-param-list={declared}&q-signature={sigv}")

    def _request(self, method: str, path: str, params: dict,
                 data: Optional[bytes], timeout: float,
                 extra_headers: Optional[dict] = None):
        """单次签名请求 + 瞬态退避重试；重试耗尽后上抛（调用方止步）。

        extra_headers 仅随请求发送、不参与签名（签名固定只覆盖 host），
        用于 If-None-Match 等条件头；对应后端按未签名头放行的契约。
        """
        q = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(str(v), safe='')}"
                     for k, v in sorted(params.items()))
        url = f"https://{self.host}{path}" + (f"?{q}" if q else "")
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                headers = {"authorization": self._sign(method, path, params)}
                if extra_headers:
                    headers.update({k: str(v) for k, v in extra_headers.items()})
                st, hdrs, body = self._transport(
                    method, url, headers, data, timeout)
                if st not in _RETRY_STATUS:
                    return st, hdrs, body
                last_exc = RuntimeError(f"COS {st} ({method} {path})")
            except Exception as e:                     # 传输层错误同轨重试
                last_exc = e
            if attempt < self.retries - 1:
                self._sleep(self.backoff[min(attempt, len(self.backoff) - 1)])
        raise last_exc if last_exc else RuntimeError("COS 重试耗尽")

    # ---- 对象级原语 ----

    def call(self, method: str, key: str, data: Optional[bytes] = None,
             timeout: float = 300.0, headers: Optional[dict] = None):
        path = key if key.startswith("/") else "/" + key
        return self._request(method, urllib.parse.quote(path), {}, data,
                             timeout, extra_headers=headers)

    def get_bytes(self, key: str) -> Optional[bytes]:
        st, _, body = self.call("GET", key, timeout=120.0)
        return body if st == 200 else None

    def put_bytes(self, key: str, data: bytes) -> Optional[str]:
        """上传小对象，成功返回 ETag（内容校验用），非 2xx 返回 None。"""
        st, hdrs, _ = self.call("PUT", key, data=data, timeout=300.0)
        return hdrs.get("ETag") if st in (200, 204) else None

    def head(self, key: str) -> Optional[int]:
        """HEAD 取 Content-Length（存在性/同尺寸跳过的咨询语义）。"""
        path = key if key.startswith("/") else "/" + key
        try:
            st, hdrs, _ = self._request("HEAD", urllib.parse.quote(path), {},
                                        None, 60.0)
        except Exception:
            return None
        if st != 200:
            return None
        try:
            return int(hdrs.get("Content-Length", ""))
        except (TypeError, ValueError):
            return None

    def delete(self, key: str) -> None:
        """删除；404 视为已删（幂等收尾）。"""
        try:
            self.call("DELETE", key, timeout=60.0)
        except RuntimeError:
            pass

    # ---- 列举 ----

    def list_prefix(self, prefix: str, max_keys: int = 1000) -> list:
        keys: list = []
        marker = ""
        while True:
            params = {"prefix": prefix, "max-keys": str(max_keys)}
            if marker:
                params["marker"] = marker
            _, _, body = self._request("GET", "/", params, None, 60.0)
            t = body.decode(errors="replace")
            page = [m.group(1) for m in re.finditer(r"<Key>([^<]+)</Key>", t)]
            keys.extend(page)
            if "<IsTruncated>true</IsTruncated>" not in t:
                return keys
            nm = re.search(r"<NextMarker>([^<]+)</NextMarker>", t)
            marker = nm.group(1) if nm else (page[-1] if page else "")
            if not marker:
                return keys

    def list_entries(self, prefix: str, max_keys: int = 1000):
        """分页列举，产出 ``(key, size, etag, last_modified)``。

        etag 去引号/去 ``-N`` 分片后缀由调用方解释（坑：分片对象 ETag 非 md5）。
        """
        marker = ""
        while True:
            params = {"prefix": prefix, "max-keys": str(max_keys)}
            if marker:
                params["marker"] = marker
            _, _, body = self._request("GET", "/", params, None, 60.0)
            t = body.decode(errors="replace")
            page = []
            for blk in re.finditer(r"<Contents>(.*?)</Contents>", t, re.S):
                b = blk.group(1)
                k = re.search(r"<Key>([^<]+)</Key>", b)
                s = re.search(r"<Size>(\d+)</Size>", b)
                if not (k and s):
                    continue
                et = re.search(r"<ETag>([^<]*)</ETag>", b)
                lm = re.search(r"<LastModified>([^<]*)</LastModified>", b)
                etag = et.group(1).replace("&quot;", "").strip('"') if et else ""
                page.append((k.group(1), int(s.group(1)), etag,
                             lm.group(1) if lm else ""))
            yield from page
            if "<IsTruncated>true</IsTruncated>" not in t:
                return
            nm = re.search(r"<NextMarker>([^<]+)</NextMarker>", t)
            marker = nm.group(1) if nm else (page[-1][0] if page else "")
            if not marker:
                return

    # ---- 上传（大对象） ----

    def put_multipart(self, key: str, path: str, *, part_size: int = 256 * 1024 ** 2,
                      part_retries: int = 3, on_progress=None) -> bool:
        """分片 PUT：initiate → 逐片(带重试) → complete → HEAD 对尺寸核验。

        分片对象 ETag 非 md5（坑），完整性由 HEAD Content-Length 兜底 +
        调用方在消息头/边车里自带内容 md5。part_size 默认 256MB：
        2026-09-20 冷备链实测，跨境劣化期 64MB 片三连败、256MB 片在恢复
        窗口全过（片数少 → 暴露面小）。
        ``on_progress(rate)``：每片成功 PUT 后回调该片速率（bytes/s），
        供上层进度喂狗；不抛错、不参与流控（2026-09-22 sg 推卷被 30min
        死线反复击穿同病，COS 腿无会话轮换逃生门，按片完成无条件喂）。
        """
        qpath = urllib.parse.quote("/" + key.lstrip("/"))
        st, _, body = self._request("POST", qpath, {"uploads": ""}, None, 60.0)
        if st != 200:
            return False
        m = re.search(r"<UploadId>([^<]+)</UploadId>",
                      body.decode(errors="replace"))
        if not m:
            return False
        uid, parts, no = m.group(1), [], 1
        total = os.path.getsize(path)
        with open(path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break
                for attempt in range(part_retries):
                    t0 = time.time()
                    try:
                        st, hdrs, _ = self._request(
                            "PUT", qpath, {"partNumber": str(no), "uploadId": uid},
                            chunk, 600.0)
                    except Exception:
                        st = 0
                    if st == 200:
                        parts.append(f"<Part><PartNumber>{no}</PartNumber>"
                                     f"<ETag>{hdrs.get('ETag')}</ETag></Part>")
                        if on_progress is not None:
                            try:
                                on_progress(len(chunk) / max(time.time() - t0, 1e-6))
                            except Exception:
                                pass      # 进度回调绝不影响上传主流程
                        break
                    self._sleep(2 * (attempt + 1))
                else:
                    try:
                        self._request("DELETE", qpath, {"uploadId": uid}, None, 60.0)
                    except Exception:
                        pass
                    return False
                no += 1
        xml = (f"<CompleteMultipartUpload>{''.join(parts)}"
               f"</CompleteMultipartUpload>").encode()
        st, _, _ = self._request("POST", qpath, {"uploadId": uid}, xml, 300.0)
        if st != 200:
            return False
        got = self.head(key)
        return got == total

    def put_smart(self, key: str, path: str, md5: str, *,
                  multipart_th: int = 256 * 1024 ** 2,
                  part_size: int = 256 * 1024 ** 2,
                  on_progress=None) -> bool:
        """按尺寸路由：小对象单流 PUT + ETag==md5 校验；大对象走分片。

        阈值默认 256MB（2026-09-20 冷备链实测：跨境劣化期长单流必死、
        小对象全活——大流量一律分片，小对象保留 ETag 强校验）。
        ``on_progress`` 仅分片路径有片级回调（单流 PUT 无可观测进度）。
        """
        if os.path.getsize(path) <= multipart_th:
            with open(path, "rb") as f:
                data = f.read()
            st, hdrs, _ = self._request(
                "PUT", urllib.parse.quote("/" + key.lstrip("/")), {},
                data, 600.0)
            if st != 200:
                return False
            return (hdrs.get("ETag") or "").strip('"').lower() == md5
        return self.put_multipart(key, path, part_size=part_size,
                                  on_progress=on_progress)

    def put_stream(self, key: str, fileobj, timeout: float = 600.0):
        """流式单 PUT（文件句柄直传，供小文件零拷贝路径）。"""
        return self._request("PUT", urllib.parse.quote("/" + key.lstrip("/")), {},
                             fileobj.read(), timeout)[0]

    # ---- 下载 ----

    def download_to(self, key: str, path: str, chunk: int = 8 << 20) -> bool:
        """流式 GET 落盘（分块拷贝，常驻内存 O(chunk)；1G 小机安全）。

        md5 由调用方对下载件计算；传输中断上抛，由调用方按单元级重试
        整文件重下（断点续传不做——消费语义幂等，重下最简）。
        """
        qpath = urllib.parse.quote("/" + key.lstrip("/"))
        url = f"https://{self.host}{qpath}"
        req = urllib.request.Request(
            url, method="GET",
            headers={"authorization": self._sign("GET", qpath, {})})
        with urllib.request.urlopen(req, timeout=900) as r, \
                open(path, "wb") as f:
            while True:
                buf = r.read(chunk)
                if not buf:
                    return True
                f.write(buf)
