"""Private composition fixture. Real controller/watchdog/Unix/SQLite/pidfd;
Cgroups and runtime execution are simulated. Never loads an active binding.
"""
import json
import os
from pathlib import Path
import signal
import sys
import time
from types import SimpleNamespace

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis import leases
from demiflow.workbench.analysis import priority_control
from demiflow.workbench.analysis import read_control
from demiflow.workbench.analysis import watchdog_service
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.resource_profile import profile

ID='j'+'a'*32
MEDIA_ID='j'+'b'*32


def deployment(root, auth):
    return Deployment(dict(site=dict(allowed_web_uids=[os.geteuid()], helper_gid=os.getegid(),
        priority_socket_path=str(root/'p.sock'), web_auth_socket_path=str(auth)),
        owner_release=dict(helper_sha256='a'*64), profile_sha256=profile()[1]),
        SimpleNamespace(state=root/'state', delegated_cgroup=root/'fake-cgroup', helper_gid=os.getegid()), 'production')


def watcher(root, auth, controller_pid, event_fd):
    class Groups:
        root_identity=dict(device=1,inode=2)
        jobs_identity=dict(device=1,inode=3)
        def __init__(self, _): self.alive={ID:True,MEDIA_ID:True}
        def populated(self, identifier, owned): return self.alive[identifier]
        def kill(self, identifier, owned):
            assert identifier in {ID,MEDIA_ID} and owned==dict(device=1,inode=4 if identifier==ID else 6)
            if self.alive[identifier]: os.write(event_fd,b'K' if identifier==ID else b'M')
            self.alive[identifier]=False
        def absent(self, identifier): return False
    watchdog_service.Cgroups=Groups
    watchdog_service.read_control=lambda path,name:str(controller_pid if Path(path).name=='controller' else os.getpid())
    private_directory(root/'state');private_directory(root/'state/runtime')
    service=watchdog_service.WatchdogService(deployment(root,auth))
    signal.signal(signal.SIGTERM,lambda *_:setattr(service,'running',False))
    print('ready',flush=True)
    try: service.serve()
    finally: service.close()


def controller(root, auth, watchdog_pid, web_pid, mode='controller'):
    import _forge_analysis_loop_controller as loop
    from demiflow.forge.io import atomic_write_json
    original_runtime=loop.Runtime
    captured={}
    def binding(value,paths,mode):
        value['site']['priority_socket_path']=str(root/'p.sock')
        value['profile_sha256']=profile()[1]
        paths.delegated_cgroup=root/'fake-cgroup'
        captured['deployment']=Deployment(value,paths,mode)
        return captured['deployment']
    class Runtime(original_runtime):
        def __init__(self,path):
            super().__init__(path)
            authority=SimpleNamespace(project='w'+'1'*32,principal='owner',
                peer=Peer(web_pid,os.geteuid(),os.getegid(),process_identity(web_pid)))
            client=watchdog_service.WatchClient(captured['deployment'])
            if mode != 'controller-media':
                digest=priority_control.publish(path,authority,'session','generation')
                client.owner('bind-owner',digest,record=priority_control.load(path))
                cg=dict(job_id=ID,role='native',cgroup_identity=dict(device=1,inode=4),
                        delegation_identity=dict(device=1,inode=2),jobs_identity=dict(device=1,inode=3))
                lease=dict(format=leases.FORMAT,job_id=ID,role='native',state='started',project=authority.project,
                    execution_id='execution',controller=process_identity(os.getpid()),web_parent=authority.peer.identity,
                    owner_digest=digest,delegation_identity=cg['delegation_identity'],jobs_identity=cg['jobs_identity'],
                    cgroup=cg,unit_identity=dict(device=1,inode=5),init_identity=None,reason=None,deadline=time.monotonic()+30)
                private_directory(path/ID);atomic_write_json(path/ID/'lease.json',lease)
                assert client.register(lease)
            if mode != 'controller':
                authority.project='w'+'2'*32
                authority.check=lambda *args:True  # fixture initial read grant; watchdog checks real Unix authority
                artifact=dict(id='o'+'a'*32,generation=1,digest='b'*64,sha256='c'*64,bytes=20)
                digest=read_control.publish(path,authority,'read-one',artifact)
                owner=read_control.load(path)
                assert client.read_owner('bind-read-owner',digest,record=owner)['bound']
                cg=dict(job_id=MEDIA_ID,role='media',cgroup_identity=dict(device=1,inode=6),
                        delegation_identity=dict(device=1,inode=2),jobs_identity=dict(device=1,inode=3))
                lease=dict(format=leases.MEDIA_FORMAT,owner_kind='media-read',job_id=MEDIA_ID,role='media',state='started',
                    project=authority.project,execution_id='read-one',controller=owner['controller'],web_parent=owner['web_parent'],
                    owner_digest=digest,delegation_identity=cg['delegation_identity'],jobs_identity=cg['jobs_identity'],
                    cgroup=cg,unit_identity=dict(device=1,inode=7),init_identity=None,reason=None,deadline=owner['deadline'])
                private_directory(path/MEDIA_ID);atomic_write_json(path/MEDIA_ID/'lease.json',lease)
                assert client.register(lease)
            def block(*_):
                print('persistent-io-blocked',flush=True)
                sys.stdin.buffer.read(1)
            signal.signal(signal.SIGUSR1,block)
    loop.Runtime=Runtime;loop.Deployment=binding
    loop.Watchdog=lambda:watchdog_service.WatchClient(captured['deployment'])
    watchdog_service.read_control=lambda *_:str(watchdog_pid)
    sys.argv=[sys.argv[0],str(root/'state'),str(root/'h.sock'),str(auth),str(root/'controller-proof.json')]
    loop.main()


if __name__=='__main__':
    mode=sys.argv[1]; root=Path(sys.argv[2]);auth=Path(sys.argv[3])
    if mode=='watchdog': watcher(root,auth,int(sys.argv[4]),int(sys.argv[5]))
    else:
        # Controller PID is known before watchdog is started, so the trusted
        # role-membership double never reads a pidfile during the stop loop.
        watchdog_pid=int(sys.stdin.buffer.readline())
        controller(root,auth,watchdog_pid,int(sys.argv[4]),mode)
