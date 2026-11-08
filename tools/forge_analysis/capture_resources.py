#!/usr/bin/env python3
"""Read-only bounded B1 resource sampler, explicitly authorized binding only.

No job creation, cgroup writes or process signals. Measurements are observations,
not a passing calibration receipt. Invoke only after coordinator authorization
on the administrator-provided host; never as a probe of the current container.
"""
import argparse
import json
import os
from pathlib import Path
import re
import stat
import time

LIMIT = 16*1024*1024
CONTROLS = ('memory.max','memory.current','memory.peak','memory.swap.max','memory.swap.current',
    'memory.stat','memory.events','memory.oom.group','pids.max','pids.current','pids.events','cpu.max','cpu.stat','cgroup.events','cgroup.procs')


def read(path, maximum=65536, *, optional=False):
    try:fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    except FileNotFoundError:
        if optional:return None
        raise
    try:
        data=os.read(fd,maximum+1)
        if len(data)>maximum:raise ValueError('Resource observation exceeds byte budget')
        return data.decode('ascii').strip()
    finally:os.close(fd)


def binding(path):
    value=json.loads(read(path))
    if value.get('format')!='demiflow_analysis_executor_binding_v2':raise ValueError('Exact binding v2 required')
    if value.get('binding_status') not in {'GRANTED_FOR_CALIBRATION','REVIEWED_ACTIVE'} or value.get('activation',{}).get('calibration_allowed') is not True:
        raise ValueError('Explicit administrator calibration authorization required')
    site=value['site'];root=Path(site['delegated_analysis_cgroup_root'])
    if not site.get('resource_authorization_ref') or not site.get('supervisor_policy_ref'):
        raise ValueError('Resource authorization and outer supervision policy required')
    if not root.is_absolute() or '..' in root.parts or not root.is_relative_to('/sys/fs/cgroup') or root==Path('/sys/fs/cgroup'):
        raise ValueError('Only the exact dedicated analysis cgroup is observable')
    for part in (root,*root.parents):
        if part.is_symlink():raise ValueError('Cgroup root must not use symlinks')
    return value,root


def process(pid):
    if not pid.isdigit() or not 1<=int(pid)<=2**31-1:raise ValueError('Invalid kernel PID observation')
    before=read(Path('/proc')/pid/'stat',optional=True)
    if before is None:return dict(pid=int(pid),exited=True)
    status=read(Path('/proc')/pid/'status',optional=True)
    after=read(Path('/proc')/pid/'stat',optional=True)
    start=lambda value:value.rsplit(')',1)[1].split()[19] if value else None
    if status is None or after is None or start(before)!=start(after):return dict(pid=int(pid),identity_changed_or_exited=True)
    fields={}
    for line in status.splitlines():
        key,_,value=line.partition(':')
        if key in {'VmRSS','VmHWM','VmSize','RssAnon','RssFile','RssShmem','Threads','Uid','Gid','CapEff','NoNewPrivs','Seccomp'}:fields[key]=value.strip()
    return dict(pid=int(pid),start=start(before),status=fields)


def snapshot(root):
    paths=[root,root/'controller',root/'watchdog',root/'jobs']
    with os.scandir(root/'jobs') as children:
        for item in children:
            if item.is_dir(follow_symlinks=False):
                if not re.fullmatch('j[0-9a-f]{32}',item.name) or len(paths)>=7:raise ValueError('Unexpected or excess live leaf directories')
                paths.append(Path(item.path))
    result=[];pids=set()
    for path in paths:
        before=path.stat(follow_symlinks=False)
        if not stat.S_ISDIR(before.st_mode):raise ValueError('Cgroup role is not a physical directory')
        controls={name:read(path/name,optional=name=='memory.peak') for name in CONTROLS}
        after=path.stat(follow_symlinks=False)
        if (before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino):raise ValueError('Cgroup identity changed during observation')
        members=controls['cgroup.procs'].split()
        pids.update(members)
        if len(pids)>64:raise ValueError('Observed process count exceeds candidate analysis limit')
        result.append(dict(role='analysis' if path==root else path.relative_to(root).as_posix(),
            identity=dict(device=before.st_dev,inode=before.st_ino),controls=controls))
    return dict(monotonic=time.monotonic(),cgroups=result,processes=[process(pid) for pid in sorted(pids,key=int)])


def capture(binding_path,output,seconds):
    if type(seconds) is not int or not 1<=seconds<=60:raise ValueError('Duration must be 1..60 seconds')
    value,root=binding(binding_path)
    total=0
    first=root.stat();original=(first.st_dev,first.st_ino)
    descriptor=os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        def emit(item):
            nonlocal total
            data=(json.dumps(item,sort_keys=True,separators=(',',':'))+'\n').encode()
            if len(data)>256*1024 or total+len(data)>LIMIT:raise ValueError('Resource capture storage budget exceeded; incomplete evidence')
            stream.write(data);total+=len(data)
        emit(dict(format='demiflow_analysis_resource_observations_v1',passed=False,
            helper_sha256=value['owner_release']['helper_sha256'],image_tree_sha256=value['owner_release']['worker_image_digest'],
            profile_sha256=value['profile_sha256'],boot=read('/proc/sys/kernel/random/boot_id'),
            resource_authorization_ref=value['site']['resource_authorization_ref'],isolation_verified=False))
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            current=root.stat()
            if (current.st_dev,current.st_ino)!=original:raise ValueError('Original analysis delegation changed; capture incomplete')
            emit(snapshot(root));time.sleep(.2)
        emit(dict(capture_complete=True,passed=False,calibration_review_required=True))
    return total


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding',required=True,type=Path);parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--seconds',type=int,default=30)
    args=parser.parse_args();print(json.dumps(dict(bytes=capture(args.binding,args.output,args.seconds),passed=False)))


if __name__=='__main__':main()
