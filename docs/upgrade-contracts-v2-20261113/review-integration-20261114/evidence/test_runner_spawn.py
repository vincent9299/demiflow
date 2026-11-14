import multiprocessing
from pathlib import Path
import demiflow

def imported_path(queue):
    queue.put(str(Path(demiflow.__file__).resolve()))

def test_spawn_imports_checkout_without_reentering_pytest():
    ctx=multiprocessing.get_context('spawn')
    q=ctx.Queue();p=ctx.Process(target=imported_path,args=(q,))
    p.start()
    actual=q.get(timeout=20)
    p.join(timeout=20)
    assert p.exitcode==0
    assert Path(actual).is_relative_to(Path(demiflow.__file__).resolve().parents[1])
