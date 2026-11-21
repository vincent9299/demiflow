"""Workbench project identity for existing service records."""
import hashlib
from pathlib import Path


def workspace_id(root, value):
    # Legacy IDs are derived without modifying historic workspace controls.
    return value.get('id', 'l' + hashlib.sha256(str(Path(root).absolute()).encode()).hexdigest()[:32])
