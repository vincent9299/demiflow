"""Trusted task placement; local history and managed records never dual-write."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat

from ..io import write_exclusive
from .project import ForgeError, no_symlinks, read_json, safe_id


class TaskStore:
    def __init__(self, root, *, managed=False):
        self.root = no_symlinks(Path(root).absolute())
        self.managed = managed

    @classmethod
    def external(cls, state_root, project_id, project_root):
        safe_id(project_id)
        source = no_symlinks(Path(project_root).absolute()).resolve()
        deployment = no_symlinks(Path(state_root).absolute()).resolve()
        root = deployment / "forge-development" / project_id
        if root.is_relative_to(source) or source.is_relative_to(root):
            raise ForgeError("Managed task control directory must be outside the project")
        deployment.mkdir(parents=True, exist_ok=True)
        for path in (root.parent, root):
            no_symlinks(path)
            path.mkdir(mode=0o700, exist_ok=True)
            info = path.stat()
            if not path.is_dir() or stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
                raise ForgeError("Managed task control directories require controller ownership and mode 0700")
        info = source.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise ForgeError("Managed project root must be a physical directory")
        identity = {"schema": "demiflow-managed-project-v1", "project_id": project_id,
                    "project_root": str(source), "project_identity": [info.st_dev, info.st_ino]}
        marker = root / ".project.json"
        try:
            write_exclusive(marker, json.dumps(identity, sort_keys=True))
        except FileExistsError:
            if read_json(marker) != identity:
                raise ForgeError("Managed project ID already belongs to a different project")
        return cls(root, managed=True)

    def directory(self, task_id):
        directory = no_symlinks(self.root / safe_id(task_id))
        if self.managed and directory.exists():
            info = directory.stat()
            if not directory.is_dir() or stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.geteuid():
                raise ForgeError("Managed task directories require controller ownership and mode 0700")
        return directory

    def create(self, task_id):
        directory = self.directory(task_id)
        directory.mkdir(mode=0o700 if self.managed else 0o777,
                        parents=not self.managed, exist_ok=False)
        return directory
