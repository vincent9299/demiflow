"""HTTP teardown waits for its real bounded inspection before reader cleanup."""
import http.client
import json
from pathlib import Path
import sys
import threading
import time

import psutil
import pytest

from demiflow.forge.operation_budget import OPERATION_SECONDS
from demiflow.forge.platform import limits_with
from demiflow.forge.platform_worker import capture
from demiflow.workbench.api import Handler, WorkspaceServer
from demiflow.workbench.projects import workspace


@pytest.mark.parametrize('outcome', ['success', 'inspection_error', 'disconnect'])
def test_close_drains_owned_inspection_before_closing_shared_reader(tmp_path, monkeypatch, outcome):
    root = tmp_path / 'workspace'
    workspace.init_workspace(root, name='Owned HTTP shutdown')
    ready, release = tmp_path / 'ready', tmp_path / 'release'
    events, requests, failures = [], [], []
    request_finished, close_started, closed = (threading.Event() for _ in range(3))
    app = WorkspaceServer(root)
    original_close = app.native_reader.close

    def close_reader():
        events.append(('reader_close', request_finished.is_set()))
        original_close()

    monkeypatch.setattr(app.native_reader, 'close', close_reader)
    child = '''import json, os, pathlib, sys, time
ready, release = map(pathlib.Path, sys.argv[1:3])
ready.write_text(str(os.getpid()))
while not release.exists():
    time.sleep(.01)
if sys.argv[3] == 'inspection_error':
    raise SystemExit(3)
print(json.dumps({'format':'demiflow_forge_platform_catalog_v3','owned':True}))
'''

    class OwnedInspection(Handler):
        def do_GET(self):
            requests.append(threading.current_thread())
            events.append(('socket_timeout', self.connection.gettimeout()))
            try:
                value = capture([sys.executable, '-I', '-B', '-c', child,
                                 str(ready), str(release), outcome], {}, limits_with())
                # An admitted request retains its shared resources until its
                # original subprocess and HTTP handling have both finished.
                events.append(('request_reader_closed', self.server.native_reader.closed))
                self._send(value)
            except Exception as error:
                self._failure(error)
            finally:
                events.append(('request_done', True))
                request_finished.set()

    app.RequestHandlerClass = OwnedInspection
    owner = threading.Thread(target=app.serve_forever, daemon=True)
    owner.start()
    client = http.client.HTTPConnection('127.0.0.1', app.server_port, timeout=10)
    closer = None
    identity = None

    def close_server():
        close_started.set()
        try:
            app.server_close()
        except BaseException as error:
            failures.append(error)
        finally:
            closed.set()

    try:
        client.request('GET', '/owned-inspection')
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert ready.exists(), 'original inspection never launched'
        process = psutil.Process(int(ready.read_text()))
        identity = (process.pid, process.create_time())
        assert Path(process.cwd()).name.startswith('demiflow-platform-')
        assert not request_finished.is_set()
        if outcome == 'disconnect':
            client.close()
        app.shutdown()
        owner.join(timeout=3)
        assert not owner.is_alive()
        closer = threading.Thread(target=close_server, daemon=True)
        closer.start()
        assert close_started.wait(3)
        assert not closed.wait(.15), 'server_close returned with its inspection still running'
        assert not app.native_reader.closed
        assert events == [('socket_timeout', 65.0)]
        release.write_text('complete original operation')
        assert closed.wait(10), 'owned request did not drain'
        closer.join(timeout=1)
        assert not failures
        assert requests and all(not thread.is_alive() for thread in requests)
        assert events[-2:] == [('request_done', True), ('reader_close', True)]
        assert app.native_reader.closed and app.native_reader.process is None
        if outcome != 'inspection_error':
            assert ('request_reader_closed', False) in events
        if outcome != 'disconnect':
            response = client.getresponse()
            assert response.status == (500 if outcome == 'inspection_error' else 200)
            body = json.loads(response.read())
            if outcome == 'success':
                assert body.get('owned') is True
            else:
                assert body['code'] == 'operation_failed'
        assert OPERATION_SECONDS == 60
        try:
            observed = psutil.Process(identity[0])
            assert observed.create_time() != identity[1], 'original inspection remains alive after close'
        except psutil.NoSuchProcess:
            pass
    finally:
        release.write_text('release this fixture only')
        client.close()
        if owner.is_alive():
            app.shutdown()
            owner.join(timeout=3)
        for thread in requests:
            thread.join(timeout=10)
        if closer is not None:
            closer.join(timeout=10)
        app.server_close()
