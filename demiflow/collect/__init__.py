"""demiflow 采集原语：HTTP 底座与（后续的）档位拉取/清单落盘机制。

爬虫场景支持（2026-09-04 起，自 collect_v2.infra 上移）：机制归引擎、
策略归消费方——限速表/代理名单/身份 UA 由消费方注册（见 net.SOURCE_LIMITS）。
"""

from . import net

__all__ = ["net"]
