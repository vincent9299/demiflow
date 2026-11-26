"""Neutral selector composition; owners retain their controls and fixed refs.

The resolver returns the owners' actual DatasetRef and Path, with no JSON
conversion or captured workspace control. Construct it in the calling process;
worker requests continue to carry only the original JSON selectors.
"""
from pathlib import Path
import re
from typing import Any, Callable, Protocol

from demiflow.forge.schema import ForgeError


class TableResolver(Protocol):
    def __call__(self, root: Path, control: dict, selector: str) -> tuple[dict, Any, Path]: ...


def make_resolver(registered_table: TableResolver,
                  run_result: Callable[..., tuple[dict, Any, Path]]) -> TableResolver:
    def resolve(root, control, selector):
        if isinstance(selector, str) and selector.startswith('run:'):
            parts = selector.split(':')
            if (len(parts) not in (3, 4) or len(parts) == 4
                    and not re.fullmatch(r'(?:[1-9]|[12][0-9]|3[0-2])', parts[3])):
                raise ForgeError('Invalid run output reference')
            return run_result(root, parts[1], parts[2],
                              attempt=int(parts[3]) if len(parts) == 4 else None)
        return registered_table(root, control, selector)
    return resolve
