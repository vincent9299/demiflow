"""Deliberately limited mechanical checks; semantic rules require review."""
from __future__ import annotations

import ast
import os
from pathlib import Path

from .project import (ForgeError, SKIP, MAX_FILE, WORKSPACE_STANDARDS,
                      PROJECT_LOCAL_STANDARDS, no_symlinks, snapshot_limits)
from .notebook import read_cells

NOTEBOOK_ROLES = ("pipeline-run", "pipeline-monitor", "pipeline-results")


def project_python_checks(project, name, *, facts=None):
    """Consume the same bounded local facts used by the modern snapshot."""
    if facts is None:
        facts = project.observe(name).facts
    return sorted(facts.problems)


def structural(project, name, *, observation=None):
    if project.standard(name) in WORKSPACE_STANDARDS:
        return workspace_structural(project, name, observation=observation)
    pipeline = project.pipeline(name)
    root = project.root / pipeline["path"]
    problems = []
    required = [pipeline["entry"], "README.md", "operators", "tests",
                pipeline["entry"].removesuffix("_pipeline.py") + "_debug.ipynb"]
    for part in required:
        path = no_symlinks(root / part)
        if not path.exists():
            problems.append(f"DF-02 missing {pipeline['path']}/{part}")
    for part in ("operators", "tests"):
        if (root / part).exists() and not (root / part).is_dir():
            problems.append(f"DF-02 {part} must be a directory")
    entry = root / pipeline["entry"]
    if entry.is_file():
        try:
            tree = ast.parse(entry.read_text(encoding="utf-8"))
            names = [n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
            for symbol in ("config", "run_pipeline"):
                if names.count(symbol) != 1:
                    problems.append(f"DF-02 entry requires exactly one top-level {symbol}")
        except (SyntaxError, UnicodeError) as error:
            problems.append(f"DF-02 invalid entry: {error}")
    own_module = pipeline["path"].replace("/", ".") + "." + pipeline["entry"][:-3]
    python_files = []
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in SKIP and d != "tests")
        for d in dirs:
            no_symlinks(Path(directory) / d)
        python_files += [Path(directory) / f for f in sorted(files) if f.endswith(".py")]
    for path in python_files:
        no_symlinks(path)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeError) as error:
            problems.append(f"DF-02 invalid Python {path.relative_to(project.root)}: {error}")
            continue
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""] + [f"{node.module or ''}.{a.name}" for a in node.names]
            for module in modules:
                if any(p.endswith("_pipeline") for p in module.split(".")) and not (module == own_module or module.startswith(own_module + ".")):
                    problems.append(f"DF-03 cross-pipeline import in {path.relative_to(project.root)}:{node.lineno}: {module}")
    notebook = root / required[-1]
    if notebook.is_file():
        try:
            strict_notebook = project.standard(name) != "1.0.0-candidate.1"
            notebook_limit = (96 * 1024 * 1024 if project.standard(name) in
                              {"1.0.0-candidate.3", "1.0.0-candidate.4", "1.0.0-candidate.5", "1.0.0-candidate.6"} else MAX_FILE)
            if strict_notebook and notebook.stat().st_size > notebook_limit:
                problems.append("DF-02 notebook exceeds source budget; archive large outputs as run artifacts")
            cells = read_cells(notebook, max_bytes=snapshot_limits(project.config.get('snapshot_limits'))['notebook_bytes'],
                               code_bytes=notebook_limit)
            if strict_notebook and (len(cells) != 3 or any(c.get("cell_type") != "code" for c in cells)):
                problems.append("DF-02 notebook requires exactly three code cells: run, monitor, results")
            for index, cell in enumerate(cells):
                expected = NOTEBOOK_ROLES[index] if index < 3 else None
                roles = [tag for tag in cell.get("metadata", {}).get("tags", []) if tag in NOTEBOOK_ROLES]
                if strict_notebook and (expected is None or roles != [expected]):
                    problems.append(f"DF-02 notebook cell {index} requires role tag {expected}")
                if cell.get("cell_type") != "code":
                    continue
                code = "".join(cell["source"])
                if len(code.encode("utf-8")) > MAX_FILE:
                    problems.append(f"DF-02 notebook cell {index} source exceeds source budget")
                # IPython shell/magic lines are not Python definitions.
                code = "\n".join("" if l.lstrip().startswith(("%", "!")) else l for l in code.splitlines())
                tree = ast.parse(code)
                if any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) for n in ast.walk(tree)):
                    problems.append(f"DF-02 notebook cell {index} defines execution code")
                if strict_notebook and index > 0 and any(isinstance(n, ast.Call) and
                        ((isinstance(n.func, ast.Name) and n.func.id == "run_pipeline") or
                         (isinstance(n.func, ast.Attribute) and n.func.attr == "run_pipeline"))
                        for n in ast.walk(tree)):
                    problems.append(f"DF-02 notebook cell {index} must be read-only; run_pipeline belongs in cell 0")
        except (ValueError, KeyError, TypeError, SyntaxError) as error:
            problems.append(f"DF-02 invalid notebook: {error}")
    readme = root / "README.md"
    if readme.is_file():
        text = readme.read_text(encoding="utf-8")
        if not any(marker in text for marker in ("PIPELINE_SPEC", "pipeline-v1.md", "DemiForge")):
            problems.append("DF-02 README must reference its applicable specification")
    return problems


def workspace_structural(project, name, *, observation=None):
    """Check registered roles, not execution semantics (owned by the platform)."""
    pipeline = project.pipeline(name)
    problems = []
    root = project.root
    local = project.standard(name) in PROJECT_LOCAL_STANDARDS
    paths = [pipeline['path'] + '/README.md', pipeline['path'] + '/tests',
             pipeline['entry']['path'], *pipeline['operators'], *pipeline['notebooks']]
    for relative in paths:
        if not no_symlinks(root / relative).exists():
            problems.append(f'DF-02 missing {relative}')
    if (root / pipeline['path'] / 'tests').exists() and not (root / pipeline['path'] / 'tests').is_dir():
        problems.append('DF-02 tests must be a directory')
    source_files = set() if local else {root / pipeline['entry']['path']}
    for relative in ([] if local else pipeline['operators']):
        path = root / relative
        if path.is_file():
            source_files.add(path)
        elif path.is_dir():
            for directory, dirs, files in os.walk(path, followlinks=False):
                dirs[:] = [d for d in dirs if d not in SKIP]
                for child in dirs:
                    no_symlinks(Path(directory) / child)
                source_files.update(Path(directory) / f for f in files if f.endswith('.py'))
                if len(source_files) > 20000:
                    raise ForgeError('Too many registered Python sources')
    registered = {p['entry']['path'][:-3].replace('/', '.') + '.' + p['entry']['symbol']
                  for p in project.config['pipelines'].values() if isinstance(p['entry'], dict)}
    own = pipeline['entry']['path'][:-3].replace('/', '.') + '.' + pipeline['entry']['symbol']
    if local:
        observation = observation or project.observe(name)
        problems.extend(project_python_checks(project, name, facts=observation.facts))
        for scope in ('tree', 'readonly'):
            for path in observation.coverage[scope]['unverified_ranges']:
                problems.append(f'DF-01 necessary source coverage is unverified ({scope}): {path}')

    for path in sorted(source_files):
        if not path.is_file():
            continue
        no_symlinks(path)
        relative = path.relative_to(root).as_posix()
        try:
            if path.stat().st_size > MAX_FILE:
                raise ForgeError('Python source exceeds byte budget')
            tree = ast.parse(path.read_text(encoding='utf-8'))
            if relative == pipeline['entry']['path']:
                matches = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == pipeline['entry']['symbol']]
                if len(matches) != 1:
                    problems.append('DF-02 entry requires exactly one registered top-level function')
            for node in ast.walk(tree):
                modules = []
                if isinstance(node, ast.Import):
                    modules = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    prefix = node.module or ''
                    if node.level:
                        parts = relative[:-3].split('/')[:-1]
                        prefix = '.'.join(parts[:len(parts) - node.level + 1] + ([prefix] if prefix else []))
                    modules = [prefix] + [prefix + '.' + a.name for a in node.names]
                if any(m in registered - {own} for m in modules):
                    problems.append(f'DF-03 cross-entry import in {relative}:{node.lineno}')
                if relative.startswith('operators/') and any(m == 'flows' or m.startswith('flows.') for m in modules):
                    problems.append(f'DF-03 operator imports flows in {relative}:{node.lineno}')
        except (ForgeError, SyntaxError, UnicodeError) as error:
            problems.append(f'DF-02 invalid Python {relative}: {error}')
    for relative in ([] if local else pipeline['notebooks']):
        path = root / relative
        if not path.is_file():
            continue
        try:
            for index, cell in enumerate(read_cells(path, max_bytes=snapshot_limits(project.config.get('snapshot_limits'))['notebook_bytes'])):
                if cell['cell_type'] != 'code':
                    continue
                code = '\n'.join('' if line.lstrip().startswith(('%', '!')) else line for line in ''.join(cell['source']).splitlines())
                if any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) for n in ast.walk(ast.parse(code))):
                    problems.append(f'DF-02 registered notebook {relative} cell {index} defines execution code')
        except (ForgeError, SyntaxError, UnicodeError, ValueError) as error:
            problems.append(f'DF-02 invalid notebook {relative}: {error}')
    readme = root / pipeline['path'] / 'README.md'
    if readme.is_file():
        if readme.stat().st_size > MAX_FILE:
            problems.append('DF-02 README exceeds byte budget')
        elif not any(marker in readme.read_text(encoding='utf-8') for marker in ('PIPELINE_SPEC', 'DemiForge')):
            problems.append('DF-02 README must reference its applicable specification')
    if local:
        observation.stable()
    return problems
