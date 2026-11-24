"""Test-only launch wrapper around the actual sealed Forge supervisor/worker.

No source or seal is modified. The wrapper installs only existing in-memory
fault seams and a bounded pause; all admission, execution, accounting and
resume code remains the normal Forge implementation in the sealed runtime.
"""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time

runtime_script, mode, root, run_id, *tail = sys.argv[1:]
# Sealed layout is <target>/runtime/demiflow/<package path>; depth differs
# between forge/run_worker.py and workbench/runs/worker.py.
runtime = next(p for p in Path(runtime_script).resolve().parents if p.name == 'runtime')
sys.path.insert(0, str(runtime))
from demiflow.workbench.runs import worker as run_worker

window_path = Path(root) / 'joint_fault.json'
window = json.loads(window_path.read_text())

def write_json(path,value):
    # The response pool can reach the same observation from two threads. A
    # reader must never see an empty/truncated test marker or import document.
    fd,name=tempfile.mkstemp(prefix='.joint-publish-',dir=path.parent)
    temporary=Path(name)
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(value,stream,indent=2);stream.write('\n')
            stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)

def observe(label):
    modules = {}
    for name, module in tuple(sys.modules.items()):
        if name == 'demiflow' or name.startswith('demiflow.'):
            file = getattr(module, '__file__', None)
            if file and Path(file).suffix == '.py':
                p = Path(file).resolve()
                assert p.is_relative_to(runtime), (name, str(p))
                modules[name] = dict(path=str(p.relative_to(runtime)), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
    attempt = tail[1] if mode == 'supervise' and len(tail)>1 else tail[0] if mode == 'execute' and tail else 'legacy'
    path = Path(root) / ('joint_imports_' + mode + '_' + str(attempt) + '_' + label + '.json')
    write_json(path,dict(runtime=str(runtime), modules=modules, wrapper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))

if mode == 'supervise':
    original = subprocess.Popen
    def launch(command, *args, **kwargs):
        if isinstance(command, list) and len(command) > 4 and command[3] == runtime_script and command[4] == 'execute':
            command = command[:3] + [str(Path(__file__).resolve()), runtime_script] + command[4:]
        return original(command, *args, **kwargs)
    subprocess.Popen = launch
    observe('start')
    try:
        run_worker.supervise(Path(root), run_id, int(tail[0]), int(tail[1]) if len(tail) > 1 else None)
    finally:
        observe('finish')
elif mode == 'execute':
    from demiflow.planning.dataflow_policy import RECOVERY_LIMITS
    if window.get('image_checkpoint_fixture') and 'checkpoint_rows' in RECOVERY_LIMITS:
        # Bind a deterministic row-trigger workload in each isolated attempt;
        # these defaults are recorded in the native recovery identity.
        RECOVERY_LIMITS.update(checkpoint_rows=128, checkpoint_interval_ms=60000)
    from demiflow.execution.pipeline.run_context import current_run
    import demiflow.execution.checkpoint.commit as protocol
    from demiflow.execution.outputs.object_sink import ObjectSink
    def pause(commit_id=None, run=None):
        run = run or current_run()
        fact = run.journal.execution_commit(commit_id) if commit_id else None
        observe('fault')
        from demiflow.execution.storage.state_artifacts import StateArtifacts
        cut = (StateArtifacts(run).read(fact['prepared']['state']['dataflow-cut-v3'])
               if fact and 'dataflow-cut-v3' in fact['prepared'].get('state', {}) else None)
        write_json(Path(root)/'joint_fault_ready.json',dict(window=window['window'],fact=fact,cut=cut,run_id=run.ref.run_id))
        until = time.monotonic() + 120
        while time.monotonic() < until:
            run.check_stop()
            time.sleep(.02)
        raise RuntimeError('Test fault pause deadline exceeded')
    if window['window'] == 'after_provider_before_artifact':
        ObjectSink.stage = lambda self, *a, **kw: pause(run=self.run)
    else:
        def fault(name, commit_id):
            run = current_run()
            fact = run.journal.execution_commit(commit_id)
            final=window['window'].startswith('final_')
            point=window['window'].removeprefix('final_') if final else window['window']
            from demiflow.execution.storage.state_artifacts import StateArtifacts
            state = (fact.get('prepared') or {}).get('state', {}).get('dataflow-cut-v3')
            cut = StateArtifacts(run).read(state) if state else None
            matches = bool(cut) and (
                fact['intent']['sinks'] == ['write_lance'] and cut['cut']['final'] if final else
                fact['intent']['sinks'] == ['materialize'] and not cut['cut']['final']
                and sum(source['next_sequence'] for source in cut['dataflow.sources.v3']) == 128)
            if name == point and matches:
                pause(commit_id)
        protocol.fault_point = fault
    observe('start')
    try:
        status = run_worker.execute(Path(root), run_id, int(tail[0]) if tail else None)
    finally:
        observe('finish')
    raise SystemExit(status)
else:
    raise ValueError('Unexpected real Forge launch mode')
