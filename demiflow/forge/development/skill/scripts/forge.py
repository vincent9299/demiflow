#!/usr/bin/env python3
"""Thin normal-package entry from the canonical Skill in a checkout or wheel."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))
from demiflow.forge.development.cli import main

raise SystemExit(main())
