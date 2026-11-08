from typing import TypedDict


class SelectionRow(TypedDict):
    selected: bool


def normalize_question(text: str | None, *, max_chars: int = 4096) -> str:
    """将空题面归一为空串，合并空白；拒绝超长输入。"""
    if not 1 <= max_chars <= 4096:
        raise ValueError("max_chars must be within 1..4096")
    if text is None:
        return ""
    if len(text) > max_chars:
        raise ValueError("question exceeds max_chars")
    return " ".join(text.split())


def above_threshold(score: float, *, minimum: float) -> bool:
    """评分与阈值均在 0..1，返回本条是否达标。"""
    if not 0.0 <= score <= 1.0 or not 0.0 <= minimum <= 1.0:
        raise ValueError("score and minimum must be within 0..1")
    return score >= minimum


def keep_selected(row: SelectionRow) -> bool:
    """filter 只消费 selected 字段，其余列由原生节点保留。"""
    return row["selected"]
