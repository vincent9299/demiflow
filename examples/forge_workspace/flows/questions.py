from demiflow import data
from operators.question_rules import (
    normalize_question,
    above_threshold,
    keep_selected,
)


def _select(*, candidates, scores, target, minimum):
    source = data.read_lance(
        candidates["uri"], version=candidates["version"],
        columns=["case_id", "question", "image_uri"],
    )
    normalized = source.map(
        normalize_question,
        inputs={"text": "question"},
        output="normalized_question",
        fn_kwargs={"max_chars": 4096},
    )
    grades = data.read_lance(
        scores["uri"], version=scores["version"],
        columns=["case_id", "score"],
    )
    joined = normalized.join(grades, on="case_id", how="inner")
    marked = joined.map(
        above_threshold,
        inputs={"score": "score"},
        output="selected",
        fn_kwargs={"minimum": minimum},
    )
    selected = marked.filter(keep_selected)
    return selected.write_lance(
        target["uri"], mode="create", return_receipt=True,
    )


def select_a(*, candidates, scores, target):
    """方案 A：评分至少 0.80。"""
    return _select(
        candidates=candidates, scores=scores, target=target, minimum=0.80,
    )


def select_b(*, candidates, scores, target):
    """方案 B：评分至少 0.90。"""
    return _select(
        candidates=candidates, scores=scores, target=target, minimum=0.90,
    )

def select(*, candidates, scores, target, minimum):
    """Normalize questions, join fixed scores and select a configured threshold."""
    return _select(candidates=candidates, scores=scores, target=target, minimum=minimum)
