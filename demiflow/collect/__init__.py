"""demiflow 采集原语：爬虫场景的机制层（2026-09-04 起，自 collect_v2 沉淀）。

- net：限速闸门/分类重试/双池代理/流式原语（策略注册式）；
- fetch：多候选档位轮转拉取（字节封顶/硬超时/verify 钩子）；
- store：内容寻址 blob + 追加清单 + 跨进程幂等去重；
- resume：清单现算 done-set/计数（断点续跑）；
- exec_curl / fleet（2026-09-18 上移）：短命 curl 传输 + AIMD + 出口身份；
  systemd-run 托管 fleet 发射/巡检；
- cosio / cosqueue / queue_runner（2026-09-20 第三代队列沉淀）：COS 签名
  对象存取（瞬态退避重试）；COS 任务队列（认领校验/成功才 complete/
  超龄认领回收）；认领→批算子→完成常驻循环（身份 env 注入）。

分工铁律：机制归引擎，策略与知识（源名单/限速数值/身份 UA/字段契约/
prompt）归消费方——引擎层零业务词汇（grep 断言固化）。
"""

import importlib as _il
import sys as _sys

_LAZY = ("net", "fetch", "store", "resume", "crawl", "images", "search",
         "llm", "exec_curl", "fleet", "cosio", "cosqueue", "queue_runner",
         "relay", "pan123", "supervisor", "sqlite_queue", "embedded_worker", "network_config")


def __getattr__(name):
    """惰性子模块：核心路径（cosio/relay/pan123/supervisor）零三方依赖，
    按需 import——重依赖（httpx/crawl4ai/pillow）只在真正取用对应子模块
    时才要求安装（extras 化的机制落点，机器部署零 pip）。"""
    if name in _LAZY and f"{__name__}.{name}" in _sys.modules:
        return _sys.modules[f"{__name__}.{name}"]
    if name in _LAZY:
        return _il.import_module(f"{__name__}.{name}")
    raise AttributeError(name)


def __dir__():
    return list(globals()) + list(_LAZY)


__all__ = list(_LAZY)

# Declarative native search configuration; vendor code loads only in workers.
from .native_search import SearchConfig, Secret, search_source_inventory
