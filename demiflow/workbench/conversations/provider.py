"""Provider selection is explicit; failures never trigger a fallback supplier."""

def adapter_for(name, operation="message"):
    if name=='codex':
        from demiflow.workbench.conversations.codex import Codex
        from demiflow.workbench.conversations.codex import CodexCompact
        return CodexCompact if operation=="compact" else Codex
    if name=='deepseek':
        from demiflow.workbench.conversations.deepseek import DeepSeek
        from demiflow.workbench.conversations.deepseek import DeepSeekCompact
        return DeepSeekCompact if operation=="compact" else DeepSeek
    raise ValueError('Unsupported development provider')
