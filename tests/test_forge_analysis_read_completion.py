"""Real short reader lifetime over a Unix socket, without a Runtime substitute.

Only frame receipt/liveness is covered here. Publication, permissions and
managed cleanup are exercised by the separate actual public journey.
"""
import os
from pathlib import Path
import socket
import subprocess
import sys

import pytest

from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Peer


@pytest.mark.parametrize('valid_ack',[True,False,1])
def test_short_reader_stays_live_until_its_end_is_acknowledged(tmp_path,valid_ack):
    path=tmp_path/'r.sock'
    assert len(os.fsencode(path))<=107
    child_code="""
import socket,sys
from demiflow.workbench.analysis.framing import Frames
with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
    connection.settimeout(3)
    connection.connect(sys.argv[1])
    frames=Frames(connection)
    frames.send('json',{'artifact':'fixed-original'})
    frames.send('blob',b'original-media')
    frames.end_read()
print('read-end-acknowledged')
"""
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as listener:
        listener.bind(str(path));listener.listen(1);listener.settimeout(3)
        process=subprocess.Popen([sys.executable,'-B','-c',child_code,str(path)],
            cwd=Path(__file__).resolve().parents[1],
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            connection,_=listener.accept()
            with connection:
                connection.settimeout(3)
                peer=Peer.capture(connection,allowed_uids={os.geteuid()})
                assert peer.pid==process.pid and peer.live()
                frames=Frames(connection)
                assert frames.receive()==('json',{'artifact':'fixed-original'})
                assert frames.receive()==('blob',b'original-media')
                assert frames.receive()==('end',None)
                # END is queued and fully read, but the original process must
                # still exist for the controller's per-frame peer check.
                assert process.poll() is None and peer.live()
                frames.send('json',dict(format='demiflow_analysis_read_end_v1',accepted=valid_ack))
            stdout,stderr=process.communicate(timeout=5)
            assert len(stdout)<=1024 and len(stderr)<=8192
            if valid_ack is True:
                assert process.returncode==0 and stdout==b'read-end-acknowledged\n' and not stderr
            else:
                assert process.returncode!=0 and not stdout and b'not acknowledged' in stderr
        finally:
            if process.poll() is None:
                process.terminate();process.communicate(timeout=5)
