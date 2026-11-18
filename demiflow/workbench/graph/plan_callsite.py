"""Read-only construction provenance; never an alternative graph compiler."""
import ast
from itertools import islice
from pathlib import Path
import sys
from types import FunctionType
import weakref

from demiflow.forge.platform_environment import read_bounded
from demiflow.workbench.projects import workspace as ws
from .plan_details import material, unavailable
import hashlib


class SourceCallsites:
    """Observe source objects returned by the actual SDK in one preview worker.

    At most 1024 weak references and passive locations survive construction.
    No frames, Dataset rows, arguments or user objects are retained. Binding to
    a compiled node is by source object identity, never by filename/table name.
    """
    def __init__(self, root, sources):
        from demiflow.data.api import DataAPI
        from demiflow.data.dataset import Dataset, MaterializedDataset
        self.root, self.sources = Path(root), sources
        self.codes = {v.__code__ for k, v in vars(DataAPI).items()
                      if not k.startswith('_') and type(v) is FunctionType}
        self.dataset_types = (Dataset, MaterializedDataset)
        self.locations, self.files = {}, {}

    def __enter__(self):
        self.previous = sys.getprofile()
        if self.previous is None:
            sys.setprofile(self._observe)
        return self

    def __exit__(self, *exc):
        if self.previous is None:
            sys.setprofile(None)

    def _observe(self, frame, event, returned):
        if (event != 'return' or frame.f_code not in self.codes
                or type(returned) not in self.dataset_types or len(self.locations) >= 1024):
            return
        source = object.__getattribute__(returned, '_source')
        if id(source) in self.locations:
            return
        caller = frame.f_back
        for _ in range(64):
            if caller is None:
                return
            path = Path(caller.f_code.co_filename)
            if path.is_absolute() and path.is_relative_to(self.root):
                relative = path.relative_to(self.root).as_posix()
                if relative in self.sources:
                    positions = next(islice(caller.f_code.co_positions(), caller.f_lasti // 2, None), None)
                    if positions is not None and all(type(n) is int for n in positions):
                        try:
                            ref = weakref.ref(source)
                        except TypeError:
                            return
                        self.locations[id(source)] = (ref, relative, positions)
                    return
            caller = caller.f_back

    def detail(self, source):
        saved = self.locations.get(id(source))
        if saved is None or saved[0]() is not source:
            return unavailable('这份解析未记录该数据源的精确构造调用；可查看读取参数或 Pipeline 源码。')
        _, relative, (line, end_line, column, end_column) = saved
        if relative not in self.files:
            path = self.root / relative
            ws.no_symlinks(path)
            raw = read_bounded(path, 4 * 1024**2)
            expected = self.sources[relative]
            if hashlib.sha256(raw).hexdigest() != expected:
                return unavailable('调用源码在解析期间发生变化。')
            text = raw.decode('utf-8-sig')
            self.files[relative] = (text, ast.parse(text), expected)
        text, tree, expected = self.files[relative]
        # Python's executed CALL position identifies the exact expression,
        # including chained calls, repeated readers and multiple calls per line.
        candidates = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                      and (n.lineno, n.end_lineno, n.col_offset, n.end_col_offset)
                      == (line, end_line, column, end_column)]
        if len(candidates) != 1:
            return unavailable('无法唯一定位这次数据源构造的调用表达式。')
        return material(ast.get_source_segment(text, candidates[0]), language='python', path=relative,
                        sha256=expected, start_line=line, end_line=end_line)


def pipeline_material(root, target, sources):
    """Snapshot the declared entry file, including code after the first action."""
    relative = target.relative_to(root).as_posix()
    ws.no_symlinks(target)
    raw = read_bounded(target, 4 * 1024**2)
    expected = sources[relative]
    if hashlib.sha256(raw).hexdigest() != expected:
        return unavailable('入口源码在解析期间发生变化。')
    text = raw.decode('utf-8-sig')
    return material(text, language='python', path=relative, sha256=expected,
                    start_line=1, end_line=len(text.splitlines()))
