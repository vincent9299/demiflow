from pathlib import Path
import tempfile,json,sys,traceback
import demiflow
from demiflow import data
from demiflow.execution.run_context import RunContext
root=Path('/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115')
assert Path(demiflow.__file__).resolve().is_relative_to(root)
target=Path(tempfile.mkdtemp(prefix='demiflow-gpt-public-golden-'))
try:
    with RunContext.start(target / 'run'):
        with data.local_execution():
            actual=data.from_items([{'n':0},{'n':1},{'n':2}]).map(lambda r:{'answer':r['n']+1}).take(3)
        assert actual==[{'answer':1},{'answer':2},{'answer':3}],actual
except BaseException as exc:
    print(json.dumps({'public_golden_passed':False,'error':str(exc),'type':type(exc).__name__,'worktree':str(root),'run_directory':str(target)},ensure_ascii=False),flush=True)
    traceback.print_exc()
    sys.exit(1)
print(json.dumps({'public_golden_passed':True,'actual':actual}),flush=True)
