"""Private sole-controller migration fixture, no service or native decoder."""
import json
from pathlib import Path
import sys

from demiflow.workbench.analysis.database import Database
from demiflow.workbench.analysis.database import FileSizeGuard
from demiflow.forge.schema import ForgeError

guard=FileSizeGuard(database_bytes=8*1024*1024,wal_bytes=4*1024*1024).install()
try:
    database=Database(Path(sys.argv[1]),guard=guard)
except ForgeError as error:
    print(json.dumps(dict(ok=False,code=error.code,error=str(error))),flush=True)
else:
    try:
        with database.locked() as db:
            print(json.dumps(dict(ok=True,version=db.execute('PRAGMA user_version').fetchone()[0],
                media_basis=db.execute('SELECT count(*) FROM annotation_media_basis').fetchone()[0],
                captures=db.execute('SELECT count(*) FROM analysis_media_captures').fetchone()[0],
                environments=db.execute('SELECT count(*) FROM analysis_generation_environments').fetchone()[0],
                cleanup_receipts=db.execute('SELECT count(*) FROM analysis_cleanup_receipts').fetchone()[0])),flush=True)
    finally:database.close()
