"""Project roles for pipeline authoring and independent layout diagnostics."""
from pathlib import Path
from .platform import content_digest


ROLE_DIRS = ('flows', 'operators', 'prompts', 'data_tools')
RULES = (
    ('DF-02', '每条 flow 在 flows/ 中定义一个明确入口。共用代码算子放 operators/，'
              '原生 Prompt 和 Agent 配置放 prompts/。operators/ 里的公开算子用 __demiflow_operators__ 列出，尚未接线也可发布。'
              '可复用的数据浏览、审核和标注工具放 data_tools/，用 __demiflow_data_tools__ 列出。公开入口用 docstring 说明用途、输入和输出。'),
    ('DF-03', '在 flow 内用原生 Dataset API 串联算子、模型和 I/O。业务函数或 Actor 只负责其本地逻辑。'
              '不要把子流程、模型调用或写入藏在普通算子中，也不要从一个 flow 导入另一个 flow 的入口。'),
    ('DF-05', '在版本化的 Prompt 或 Agent 文件内保留原生模型、输入和输出声明。'
              '在 flow 使用原生节点显式引用所需定义。'),
)
RULES_ID = content_digest(RULES)


def guidance():
    return '## Pipeline 开发规则\n' + '\n'.join(f'- {rule} {text}' for rule, text in RULES)


def diagnostic(rule, path, detail, *, line=None, status='invalid'):
    return dict(rule=rule, path=path, line=line, detail=detail, status=status)


def check_layout(*, changed, operators, prompts):
    """Check known roles, including actual operator/prompt use reported by the checker."""
    diagnostics, inspected = [], set()
    for path in changed:
        if Path(path).parts[0] in ROLE_DIRS:
            inspected.add(path)
    roles = {
        'operators': operators,
        'prompts': prompts,
    }
    for directory, paths in roles.items():
        for path, line in paths:
            inspected.add(path)
            if Path(path).parts[:1] != (directory,):
                diagnostics.append(diagnostic('DF-02', path,
                    f'此文件的角色要求放在 {directory}/。', line=line))
    return inspected, diagnostics
