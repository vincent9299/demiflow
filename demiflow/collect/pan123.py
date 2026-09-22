"""demiflow 123pan 开放平台客户端（自冷备链 pan123.py 沉淀，机制归引擎）。

端点字段名不一致，勿"顺手统一"（create 用 parentFileID；mkdir 用
parentID 返回 dirID；trash 用 fileIDs 数组）。

与业务的关系：本模块只做协议与传输（token/目录/上传/回收站）；根树
名、镜像布局由消费方注入。上传语义：同内容 md5 秒传幂等；同名冲突
code=1 由消费方决定 trash 旧再传；**单文件上限（10GB 量级）以
``FileTooLarge`` 类型化抛出**，消费方据此走分卷策略。

慢速重连机制（2026-09-21 实战沉淀，工程口径=慢连接剔除+会话轮换+熔断
重试）：上传首片做测速，有效速率低于 ``min_slice_rate`` 时判定为慢路
径——re-login 换 token（会话轮换）+ 本地连接全释放（本客户端每请求新
建连接，重登即等效全量重连）→ 整文件从头重试；单文件最多重置
``max_session_resets`` 次，仍慢则照常走完（由外层看门狗兜底）。每次
重置记录前后速率到 ``resets`` 列表供事后对账（区分"会话级 QoS"与
"单纯网络劣化"）。
"""
from __future__ import annotations

import datetime
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from typing import Optional

__all__ = ["Pan123Error", "Pan123FileTooLarge", "Pan123SlowPath", "Pan123Client"]

import socket

BASE = "https://open-api.123pan.com"


def install_upload_ip_filter(bad_ips: frozenset) -> None:
    """上传后端坏 IP 过滤(进程级): 123pan 存储簇 DNS 轮询多个 m8xxx.123624.com
    主机、共享 123.184.218.0/24 后端池，个别 IP 会阶段性劣化(2026-09-21 实测
    .110 毒、其余 9 个 3-5.6MB/s)。预签名 URL 的主机名随上传会话轮换，hosts
    钉不住——在解析层统一剔除坏 IP(保留原主机名, SNI/签名不变)。幂等安装。
    """
    real = socket.getaddrinfo
    if getattr(real, "_pan123_filter", False):
        return                                # 幂等: 已安装

    def filtered(host, *a, **k):
        res = real(host, *a, **k)
        try:
            good = [r for r in res if r[4][0] not in bad_ips]
            if good and len(good) < len(res):
                return good
        except Exception:
            pass
        return res

    filtered._pan123_filter = True
    socket.getaddrinfo = filtered
_TOO_LARGE_PAT = ("单文件大小超出限制", "文件大小超出限制")


class Pan123Error(Exception):
    pass


class Pan123FileTooLarge(Pan123Error):
    """单文件超平台上限（消费方应分卷后重试，勿死循环重传）。"""


class Pan123SlowPath(Pan123Error):
    """慢路径(会话重置后仍慢): 消费方应让路(歇再战/换窗口), 勿磨。"""

    def __init__(self, rate: float):
        super().__init__(f"slow path {rate / 1024:.0f}KB/s after resets")
        self.rate = rate


class _SlowPath(Exception):
    """内部信号：首片测速低于阈值，请求外层做会话重置。"""

    def __init__(self, rate: float):
        super().__init__(f"slow first slice {rate / 1024:.0f}KB/s")
        self.rate = rate


class Pan123Client:
    """token 自动管理（过期重登）；creds/token 路径注入。"""

    def __init__(self, creds_path: str, token_path: str,
                 min_slice_rate: float = 0.5 * 1024 * 1024,
                 max_session_resets: int = 2):
        self.creds_path, self.token_path = creds_path, token_path
        # 慢速重连参数: 首片速率低于此(B/s)触发会话重置; 单文件重置上限
        self.min_slice_rate = min_slice_rate
        self.max_session_resets = max_session_resets
        self.slow_check_min = 64 * 1024 * 1024   # 仅大文件启用慢路径判定, 小件慢窗口照传
        self.resets: list[dict] = []      # 每次重置的前后速率对账

    # ---- 传输 ----

    def _http(self, method, url, headers=None, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        if data:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read().decode())
        if resp.get("code") != 0:
            msg = str(resp.get("message", ""))
            if any(p in msg for p in _TOO_LARGE_PAT):
                raise Pan123FileTooLarge(
                    f"code={resp.get('code')} message={msg}")
            raise Pan123Error(f"API错误 code={resp.get('code')} message={msg}")
        return resp["data"]

    def _auth_header(self):
        if not os.path.exists(self.token_path):
            raise Pan123Error("token 不存在, 先 login()")
        with open(self.token_path) as f:
            tok = json.load(f)
        if tok.get("expiredAt", "") < datetime.datetime.now().astimezone().isoformat():
            raise Pan123Error("token 已过期")
        return {"Authorization": f"Bearer {tok['accessToken']}",
                "Platform": "open_platform"}

    # ---- 会话 ----

    def login(self):
        with open(self.creds_path) as f:
            c = json.load(f)
        data = self._http("POST", f"{BASE}/api/v1/access_token",
                          headers={"Platform": "open_platform"},
                          body={"clientID": c["clientID"],
                                "clientSecret": c["clientSecret"]})
        with open(self.token_path, "w") as f:
            json.dump(data, f)
        os.chmod(self.token_path, 0o600)

    def _authed(self, method, url, body=None, retries_login=1):
        try:
            return self._http(method, url, headers=self._auth_header(), body=body)
        except Pan123Error as e:
            if retries_login and ("过期" in str(e) or "不存在" in str(e)):
                self.login()
                return self._authed(method, url, body, retries_login=0)
            raise

    # ---- 目录 ----

    def list_dir(self, parent_id: int = 0, search: Optional[str] = None):
        out, last = [], None
        while True:
            url = f"{BASE}/api/v2/file/list?parentFileId={parent_id}&limit=100"
            if last is not None:
                url += f"&lastFileId={last}"
            if search:
                url += f"&searchData={urllib.parse.quote(search)}"
            data = self._authed("GET", url)
            out += [x for x in data.get("fileList", []) if not x.get("trashed")]
            last = data.get("lastFileId", -1)
            if last == -1 or not data.get("fileList"):
                return out

    def mkdir(self, parent_id: int, name: str) -> int:
        return self._authed("POST", f"{BASE}/upload/v1/file/mkdir",
                            body={"parentID": parent_id, "name": name})["dirID"]

    def dir_id(self, parent_id: int, name: str) -> int:
        """按名找子目录，不存在则新建（幂等，守护进程逐级造目录用）。"""
        for x in self.list_dir(parent_id):
            if x["type"] == 1 and x["filename"] == name:
                return x["fileId"]
        return self.mkdir(parent_id, name)

    def find_by_name(self, parent_id: int, name: str) -> Optional[int]:
        """按名找直接子文件(非目录)的 fileId；供同名冲突 trash 旧再传。"""
        for x in self.list_dir(parent_id):
            if x["type"] != 1 and x["filename"] == name:
                return x["fileId"]
        return None

    def trash(self, file_ids) -> None:
        ids = file_ids if isinstance(file_ids, list) else [file_ids]
        self._authed("POST", f"{BASE}/api/v1/file/trash", body={"fileIDs": ids})

    def get_url(self, file_id: int) -> str:
        return self._authed(
            "GET", f"{BASE}/api/v1/file/download_info?fileId={file_id}"
        )["downloadUrl"]

    # ---- 上传 ----

    @staticmethod
    def md5_file(path: str) -> str:
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    def _put_slice(self, url: str, chunk: bytes) -> None:
        req = urllib.request.Request(url, data=chunk, method="PUT")
        req.add_header("Content-Type", "application/octet-stream")
        with urllib.request.urlopen(req, timeout=600) as r:
            r.read()

    def upload_file(self, path: str, parent_file_id: int,
                    name: Optional[str] = None,
                    on_progress=None) -> dict:
        """create{etag=md5} → 秒传或分片 PUT → complete/异步轮询。

        返回 ``{fileID, reuse}``；同内容秒传幂等；整文件级重试安全。
        超单文件上限抛 :class:`Pan123FileTooLarge`。
        慢速重连：首片有效速率低于阈值 → re-login 换 token → 整文件
        重来（新 preuploadID、全新连接），单文件至多 ``max_session_resets``
        次；每次重置记入 ``self.resets`` 供对账。
        ``on_progress(rate)``：每片成功 PUT 后回调该片速率（bytes/s），
        供上层进度喂狗/心跳；不抛错、不参与流控。
        """
        for reset_no in range(self.max_session_resets + 1):
            try:
                return self._upload_once(
                    path, parent_file_id, name,
                    check_slow=os.path.getsize(path) >= self.slow_check_min,
                    on_progress=on_progress)
            except _SlowPath as slow:
                before = slow.rate
                if reset_no < self.max_session_resets:
                    self.login()                 # 会话轮换=连接全释放(本客户端每请求新建连接)
                    self.resets.append({"ts": time.time(), "file": name or path,
                                        "before_bps": before})
                    continue
                raise Pan123SlowPath(before) from None   # 重置耗尽仍慢: 让路
        raise Pan123Error("unreachable")

    def _upload_once(self, path: str, parent_file_id: int, name: Optional[str],
                     check_slow: bool, on_progress=None) -> dict:
        name = name or os.path.basename(path)
        size = os.path.getsize(path)
        etag = self.md5_file(path)
        data = self._authed("POST", f"{BASE}/upload/v1/file/create",
                            body={"parentFileID": parent_file_id,
                                  "filename": name, "etag": etag, "size": size})
        if data.get("reuse"):
            return {"fileID": data.get("fileID"), "reuse": True}
        pre, slice_size = data["preuploadID"], int(data["sliceSize"])
        slices = (size + slice_size - 1) // slice_size
        with open(path, "rb") as f:
            for no in range(1, slices + 1):
                chunk = f.read(slice_size)
                for attempt in range(3):
                    try:
                        u = self._authed(
                            "POST", f"{BASE}/upload/v1/file/get_upload_url",
                            body={"preuploadID": pre, "sliceNo": no}
                        )["presignedURL"]
                        t0 = time.time()
                        self._put_slice(u, chunk)
                        rate = len(chunk) / max(time.time() - t0, 1e-6)
                        if on_progress is not None:
                            try:
                                on_progress(rate)
                            except Exception:
                                pass          # 进度回调绝不影响上传主流程
                        # 首片测速: 慢路径且还有重置额度 → 抛慢让外层换会话重来
                        if (no == 1 and check_slow
                                and rate < self.min_slice_rate):
                            raise _SlowPath(rate)   # 末轮由外层转 Pan123SlowPath
                        break
                    except _SlowPath:
                        raise
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(2 * (attempt + 1))
        r = self._authed("POST", f"{BASE}/upload/v1/file/upload_complete",
                         body={"preuploadID": pre})
        for _ in range(60):
            if r.get("completed"):
                break
            time.sleep(2)
            r = self._authed("POST",
                             f"{BASE}/upload/v1/file/upload_async_result",
                             body={"preuploadID": pre})
        if not r.get("completed"):
            raise Pan123Error(f"上传合并未完成 preuploadID={pre}")
        if self.resets:                      # 对账: 本次重置的恢复后速率
            self.resets[-1]["after_done"] = True
        return {"fileID": r.get("fileID"), "reuse": False}
