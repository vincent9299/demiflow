"""Qualified provider runtime versions.

业务流水线只接受**经过验收**的 provider 运行时版本；此前版本号散落在
`codex.py`、`review.py`、`policy.py` 三处硬编码，升级一次要改三处代码。

这里集中成单一来源，并允许用环境变量覆盖（逗号分隔），这样升级到已验收的新
版本只需要改部署配置，不必改代码：

    DEMIFLOW_CODEX_VERSIONS=0.164.0,0.170.0
    DEMIFLOW_DEEPSEEK_VERSIONS=0.2.1-rc.2

默认值只包含历史上已通过验收的版本；新增版本（例如 0.170.0）完成适配器验收后，
通过环境变量在部署侧启用，或验收通过后加入默认列表——不能仅凭版本号放行。
"""
from __future__ import annotations

import os

DEFAULT_VERSIONS = {
    'codex': ('0.164.0',),
    'deepseek': ('0.2.1-rc.2',),
}

_ENV_TEMPLATE = 'DEMIFLOW_{}_VERSIONS'


def qualified_versions(name: str) -> tuple:
    """返回该 provider 当前认可的版本集合（环境变量优先）。"""
    override = os.environ.get(_ENV_TEMPLATE.format(name.upper()), '')
    values = tuple(item.strip() for item in override.split(',') if item.strip())
    return values or DEFAULT_VERSIONS[name]


def is_qualified(name: str, version) -> bool:
    """精确版本是否在白名单内。"""
    return isinstance(version, str) and version in qualified_versions(name)


def matches_user_agent(name: str, user_agent) -> bool:
    """userAgent 里是否包含某个已认可版本（app-server 的 initialize 结果）。"""
    if not isinstance(user_agent, str):
        return False
    return any(version in user_agent for version in qualified_versions(name))
