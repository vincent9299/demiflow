"""Private real Jupyter -> compiler -> broker -> native SQL -> Ingress fixture.

Only the outer authority/broker routing and fixed mount paths are test adapters.
No OCI/B1 isolation, production notebook service or real model is claimed.
"""
import hashlib
import os
from pathlib import Path
from queue import Empty
import socket
import sys
import tempfile
import threading
import time


def jupyter_sql(root, monkeypatch):
    import lance
    import pyarrow as pa
    from jupyter_client import KernelManager
    from demiflow.workbench.analysis import query, result_transfer, kernel_worker
    from demiflow.workbench.analysis.framing import Frames
    from demiflow.workbench.analysis.object_files import Ingress
    from demiflow.workbench.analysis.jupyter_protocol import ExecutionOutput
    from demiflow.workbench.analysis.output_messages import jupyter_envelope

    tree = Path(__file__).resolve().parents[1]
    sockets = tempfile.TemporaryDirectory(prefix='demiflow-sql-', dir='/tmp')
    socket_root = Path(sockets.name)
    oid, published = 'o'+'a'*32, 'o'+'b'*32
    source = root/'sources'/oid/'data.lance'
    source.parent.mkdir(parents=True)
    lance.write_dataset(pa.table({'bucket':['a','b','a'], 'value':[2,7,3]}), str(source))
    source_digest = {str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in source.rglob('*') if p.is_file()}
    for path in ('broker','scratch/native','scratch/diagnostics'):
        (root/path).mkdir(parents=True, exist_ok=True)
    # Fixed path relocation only. The real entrypoint guard remains covered by
    # test_forge_analysis_query_contract; we invoke the already-admitted worker
    # function here with trusted fixture SQL, never install a production fallback.
    def mapped(path):
        p = Path(path)
        assert p.is_absolute() and '..' not in p.parts
        if str(p).startswith('/broker/'):
            return str(socket_root/p.name)
        return str(root/str(p).lstrip('/'))
    monkeypatch.setattr(query, 'job_path', mapped)
    monkeypatch.setattr(result_transfer, 'job_path', mapped)
    observations, errors = {}, []
    listeners = []
    for name in ('data.sock','result.sock'):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(60); sock.bind(str(socket_root/name)); sock.listen(2)
        listeners.append(sock)

    def receive():
        connection, _ = listeners[1].accept()
        with connection:
            connection.settimeout(60); frames = Frames(connection)
            request = frames.json()
            assert request['operation'] == 'native-result'
            candidate = request['candidate']
            seed = dict(project='w'+'c'*32, object_id=published, generation=1,
                retention='pinned', execution_id='sql-one', parents=[], source_binding=None)
            ingress = Ingress(root/'state', publish_id=candidate['publish_id'], seed=seed,
                description=candidate['description'], files=candidate['files'])
            frames.send('json', {'accepted':True})
            for _ in candidate['files']:
                header = frames.json(); ingress.start_file(header['file'])
                assert header['bytes'] == candidate['files'][header['file']]['bytes']
                while True:
                    kind, raw = frames.receive()
                    if kind == 'end':
                        break
                    assert kind == 'blob'; ingress.write(raw)
                ingress.finish_file()
            assert frames.receive() == ('end', None)
            ingress.seal(); ingress.rename(authorize=lambda *_: True)
            path = ingress.objects_root/published/'data.lance'
            rows = lance.dataset(str(path), version=1).to_table().to_pylist()
            observations.update(rows=rows, manifest=ingress.manifest, manifest_digest=ingress.digest,
                handle=dict(id=published,generation=1,digest=ingress.digest,rows=len(rows),
                    schema=candidate['description']['schema'],retention='pinned'))
            frames.send('json', dict(committed=True, manifest_digest=ingress.digest))

    def broker():
        connection, _ = listeners[0].accept()
        with connection:
            connection.settimeout(60); frames = Frames(connection)
            request = frames.json()
            assert request['operation'] == 'query' and request['request_id'] == 'sql-one'
            assert request['payload']['sources'] == {}
            config = dict(operation='query', sql=request['payload']['sql'], publish_id='p'+'d'*32,
                sources=[dict(alias='numbers', object_id=oid)])
            # The original production function executes real bounded DataFusion
            # in its native subprocess and streams real files to real Ingress.
            receipt = query.run_query(config)
            assert receipt['manifest_digest'] == observations['manifest_digest']
            observations.update(broker_request=request, native_receipt=receipt)
            frames.send('json', observations['handle'])
        # Actual TableHandle rich display verifies its live handle with the
        # broker; do not replace the production MIME method with fake output.
        connection, _ = listeners[0].accept()
        with connection:
            connection.settimeout(60); frames = Frames(connection)
            info = frames.json()
            assert info['operation'] == 'handle-info'
            assert info['payload']['handle'] == {k:observations['handle'][k] for k in ('id','generation','digest')}
            observations['handle_info_request'] = info
            frames.send('json', observations['handle'])

    def guarded(function):
        try: function()
        except BaseException as exc: errors.append(repr(exc))

    threads = [threading.Thread(target=guarded,args=(fn,),daemon=True) for fn in (receive,broker)]
    for thread in threads: thread.start()
    manager = KernelManager(connection_file=str(root/'kernel.json'), transport='ipc', ip=str(socket_root/'ipc'))
    client = None
    sql = 'SELECT bucket, SUM(value) AS total FROM numbers GROUP BY bucket ORDER BY bucket'
    command = dict(operation='execute',execution_id='sql-one',generation='private-sql',cell_id='sql',
        source=sql,source_sha256=hashlib.sha256(sql.encode()).hexdigest(),language='sql',result_name='summary')
    compiled = kernel_worker.execution_command(command, generation='private-sql')
    bootstrap = f'''import sys, socket
from pathlib import Path
sys.path.insert(0, {str(tree)!r})
import demiflow
assert Path(demiflow.__file__).resolve().is_relative_to(Path({str(tree)!r}))
from demiflow.workbench.analysis import client as _api
def _connect():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(60)
    sock.connect({str(socket_root/'data.sock')!r})
    return sock
_api._default._connect = _connect
'''
    env = dict(os.environ, PYTHONPATH=str(tree), PYTHONDONTWRITEBYTECODE='1', HOME=str(root),
        IPYTHONDIR=str(root/'ipython'), JUPYTER_CONFIG_DIR=str(root/'jupyter'),
        OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    try:
        manager.start_kernel(cwd=str(root), env=env)
        client = manager.client(); client.start_channels(); client.wait_for_ready(timeout=20)
        msg_id = client.execute(bootstrap+compiled,allow_stdin=False,store_history=False,stop_on_error=True)
        output = ExecutionOutput(generation='private-sql', msg_id=msg_id)
        deadline = time.monotonic()+90
        while not output.complete and time.monotonic()<deadline:
            for channel, read in (('iopub',client.get_iopub_msg),('shell',client.get_shell_msg)):
                try: message = read(timeout=.05)
                except Empty: continue
                output.consume(channel,jupyter_envelope(message),generation='private-sql')
        assert output.complete and output.status == 'succeeded', (output.snapshot(), errors)
        for thread in threads: thread.join(timeout=5)
        assert not errors and not any(t.is_alive() for t in threads), errors
        assert source_digest == {str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source.rglob('*') if p.is_file()}, 'SQL mutated its fixed input'
        return dict(observations, kernel_status=output.status, kernel_output=output.snapshot(),
            compiled_sql=compiled, source_digest=source_digest,
            scope='real private Jupyter/DataFusion/Lance/Ingress; outer authority and broker alias routing are explicit fixture adapters, not B1/OCI')
    finally:
        if client is not None: client.stop_channels()
        manager.shutdown_kernel(now=True)
        for listener in listeners: listener.close()
        for thread in threads: thread.join(timeout=2)
        sockets.cleanup()
        assert not manager.is_alive()
