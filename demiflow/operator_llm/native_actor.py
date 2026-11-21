"""Runtime identity of the native Prompt actor used by engine bindings."""
import sys


def is_native_prompt_actor(value: object) -> bool:
    """Recognize only the canonical actor, without loading a model for ordinary callables."""
    runtime = sys.modules.get("demiflow.operator_llm.runtime")
    if runtime is None or "PromptActor" not in vars(runtime):
        return False
    return type(value) is runtime.PromptActor
