"""More than the registry hard ceiling in lifetime allocations, bounded live set."""
import json
from pathlib import Path
import sys
import time

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal

root = Path(sys.argv[1])
started = time.monotonic()
peak = 0
allocations = 65568
with RunContext.start(root, limits={'timeout_s': 600, 'max_artifacts': 128}) as run:
    scratch = root/'scratch'
    scratch.mkdir()
    for offset in range(0, allocations, 32):
        with run.temporary_artifacts() as allocate:
            for i in range(offset, min(offset+32, allocations)):
                path = scratch/('private-'+str(i))
                allocate(path)
                path.write_bytes(b'x')
                path.unlink()
            with run.journal._connection() as db:
                peak = max(peak, db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0])
        if offset % 8192 == 0:
            print(json.dumps({'allocated': min(offset+32, allocations), 'peak_registry': peak}), flush=True)
    with run.journal._connection() as db:
        live = db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0]
assert live == 0 and peak <= 64
print(json.dumps({'allocations': allocations, 'registry_limit': 128, 'peak_registry': peak,
                  'final_registry': live, 'seconds': time.monotonic()-started}), flush=True)
