"""Discover Lance directories inside the project and its registered data locations."""
from collections import deque
import os
from pathlib import Path

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import workspace as ws

SKIP = {'__pycache__', 'node_modules', 'venv', 'site-packages'}
MAX_ENTRIES = 4096
MAX_TABLES = 256


def discover(root):
    root, control = ws.load_workspace_control(root)
    bases = {name: ws.location_root(root, item) for name, item in control['locations'].items()}
    # Give each explicitly configured root its own share of the scan budget.
    roots = {**bases, None: root}
    tables, issues, seen = [], [], set()
    for name, base in roots.items():
        if base in seen:
            continue
        seen.add(base)
        pending = deque([(base, 0)])
        remaining = MAX_ENTRIES // len(roots)
        while pending and remaining > 0 and len(tables) < MAX_TABLES:
            parent, depth = pending.popleft()
            try:
                ws.no_symlinks(parent)
                children = []
                with os.scandir(parent) as entries:
                    for entry in entries:
                        remaining -= 1
                        if remaining < 0:
                            break
                        if (not entry.name.startswith('.') and entry.name not in SKIP and
                                entry.is_dir(follow_symlinks=False)):
                            children.append(Path(entry.path))
                for child in sorted(children):
                    if child in roots.values() or child in seen:
                        continue
                    if child.suffix != '.lance':
                        if depth < 4:
                            pending.append((child, depth + 1))
                        continue
                    if len(tables) == MAX_TABLES:
                        break
                    relative = child.relative_to(base).as_posix()
                    if len(relative.encode()) > 1024:
                        continue
                    seen.add(child)
                    label = child.relative_to(root).as_posix() if child.is_relative_to(root) else f'{name}/{relative}'
                    digest = content_digest([str(root), name, str(base), relative])
                    tables.append(dict(id='l'+digest[:24], expected=digest, path=label, uri=str(child),
                                       location=name, relative=relative, status='available', version=None,
                                       evidence=['workspace.json:locations/'+name if name else 'project_directory']))
            except (ValueError, OSError) as exc:
                issues.append(f'{name or "项目"} · {str(exc)[:160]}')
            if len(issues) >= 16:
                return dict(tables=tables, diagnostics=issues)
        if remaining <= 0 or len(tables) >= MAX_TABLES:
            issues.append(f'{name or "项目"} 的目录较大，仅显示扫描范围内的表。')
    return dict(tables=tables, diagnostics=issues)


def resolve(root, identity, expected):
    row = next((item for item in discover(root)['tables'] if item['id'] == identity), None)
    if not row or row['expected'] != expected:
        raise ForgeError('数据目录已变更，请刷新数据字典。', code='conflict')
    return row, ws.no_symlinks(Path(row['uri']))
