"""Read-only three-way preflight against the actual working-tree baseline."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None

def main():
    target=Path(sys.argv[1]).resolve()
    baseline=json.loads((ROOT/'BASELINE.json').read_text())['files']
    files=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=ROOT).decode().split('\0')
    report={'checked_at_utc':datetime.now(timezone.utc).isoformat(),
            'source_head':json.loads((ROOT/'BASELINE.json').read_text())['source_head'],
            'target':str(target),'safe':[],'already_equal':[],'needs_three_way_merge':[]}
    for name in sorted(set(files)|set(baseline)):
        if not name or name=='BASELINE.json':continue
        before=baseline.get(name); ours=sha(ROOT/name)
        if ours==before:continue
        current=sha(target/name)
        key='already_equal' if current==ours else ('safe' if current==before else 'needs_three_way_merge')
        report[key].append(name)
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
