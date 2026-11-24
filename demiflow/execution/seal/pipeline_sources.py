"""Pure static dependency closure for one frozen Pipeline package."""
from __future__ import annotations
import ast
from pathlib import Path
from typing import Mapping

def reachable_pipeline_sources(bundle_root: str|Path,entrypoint:str)->tuple[Path,...]:
    pipeline=(Path(bundle_root).resolve()/"pipeline").resolve()
    module=str(entrypoint or "").partition(":")[0]
    if not module or any(not part.isidentifier() for part in module.split(".")):raise ValueError("Pipeline entrypoint module is invalid")
    pending=[module];visited=set();output=[]
    while pending:
        current=pending.pop()
        if current in visited:continue
        visited.add(current);path=_module_path(pipeline,current);output.append(path)
        tree=ast.parse(path.read_text(encoding="utf-8"),filename=str(path));package=_package_name(current,path)
        for node in tree.body:
            if isinstance(node,ast.ImportFrom) and node.level:
                for target in reversed(_relative_targets(node,package,pipeline)):
                    if target not in visited:pending.append(target)
    return tuple(sorted(output))

def source_map(bundle_root:str|Path,entrypoint:str)->Mapping[str,str]:
    root=Path(bundle_root).resolve();return {str(path.relative_to(root)).replace("\\","/"):path.read_text(encoding="utf-8") for path in reachable_pipeline_sources(root,entrypoint)}

def uses_lance(bundle_root:str|Path,entrypoint:str)->bool:
    methods={"read_lance","vector_search_lance","write_lance"}
    return any(any(isinstance(node,ast.Attribute) and node.attr in methods for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"),filename=str(path)))) for path in reachable_pipeline_sources(bundle_root,entrypoint))

def _module_path(pipeline:Path,module:str)->Path:
    base=pipeline.joinpath(*module.split("."));candidates=(base.with_suffix(".py"),base/"__init__.py")
    target=next((item for item in candidates if item.is_file() and not item.is_symlink()),None)
    if target is None:raise ValueError(f"Pipeline module is unavailable: {module}")
    resolved=target.resolve()
    try:resolved.relative_to(pipeline)
    except ValueError as exc:raise ValueError("Pipeline module escapes package") from exc
    return resolved

def _package_name(module:str,path:Path)->str:
    return module if path.name=="__init__.py" else module.rpartition(".")[0]

def _module_exists(pipeline:Path,module:str)->bool:
    try:_module_path(pipeline,module);return True
    except ValueError:return False

def _relative_targets(node:ast.ImportFrom,package:str,pipeline:Path)->tuple[str,...]:
    base=package.split(".") if package else []
    if node.level>len(base)+1:raise ValueError("Pipeline relative import escapes package")
    prefix=base[:len(base)-node.level+1];parts=(node.module or "").split(".") if node.module else []
    root=".".join([*prefix,*parts]).strip(".");targets=[]
    if root and _module_exists(pipeline,root):targets.append(root)
    for alias in node.names:
        if alias.name=="*":raise ValueError("Pipeline wildcard relative imports are forbidden")
        candidate=".".join(filter(None,(root,alias.name)))
        if candidate and _module_exists(pipeline,candidate):targets.append(candidate)
    return tuple(dict.fromkeys(targets))

__all__=["reachable_pipeline_sources","source_map","uses_lance"]
