"""The one project-local launcher template; no command implementation here."""
from .project import MAX_TOTAL

SCRIPT = '''#!/usr/bin/env python3
"""Project-local entry; only runs commands explicitly supplied by the caller."""
import json
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True

def config_error(message):
    print("demiforge: " + message, file=sys.stderr)
    raise SystemExit(2)

root = Path(__file__).resolve().parent.parent
CONFIG_LIMIT = @CONFIG_LIMIT@
descriptor = os.open(root / ".demiforge/project.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
with os.fdopen(descriptor, "rb") as stream:
    before = os.fstat(stream.fileno())
    if not stat.S_ISREG(before.st_mode):
        config_error("expected a regular project.json")
    if before.st_size > CONFIG_LIMIT:
        config_error("project.json exceeds configuration byte limit (" + str(CONFIG_LIMIT) + ")")
    data = stream.read(before.st_size + 1)
    after = os.fstat(stream.fileno())
    identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if len(data) != before.st_size or identity(before) != identity(after):
        config_error("project.json changed during bounded reading")
config = json.loads(data.decode("utf-8"))
tool = (root / config["tool_root"]).resolve()
sys.path.insert(0, str(tool.parent if tool.name == "demiflow" and (tool / "__init__.py").is_file() else tool))
from demiflow.forge.development.cli import main
raise SystemExit(main(["--project", str(root), *sys.argv[1:]]))
'''.replace("@CONFIG_LIMIT@", str(MAX_TOTAL))
