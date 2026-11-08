#!/usr/bin/env python3
"""Private ffprobe8.1.3 recipe from an explicitly reviewed LOCAL source archive.

No downloads/installation; one make job, no network/devices/encoders/muxers,
fixed input protocols and codec/container allowlist. Output is a candidate;
signature/source review and real isolated behavior remain separate gates.
Run only under the shared build/test flock. Missing source hash is fatal.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import resource
import shutil
import signal
import subprocess
import tarfile
import time

VERSION='8.1.3'
CONFIGURE=('--disable-autodetect','--disable-network','--disable-everything','--disable-doc',
    '--disable-debug','--disable-shared','--enable-static','--disable-x86asm','--disable-ffmpeg',
    '--disable-ffplay','--enable-ffprobe','--disable-avdevice','--disable-avfilter','--disable-swscale',
    '--enable-swresample','--enable-protocol=file,pipe',
    '--enable-demuxer=wav,mp3,ogg,matroska,mov',
    '--enable-decoder=pcm_u8,pcm_s16le,pcm_s24le,mp3,vorbis,opus,aac,h264,vp8,vp9,theora',
    '--enable-parser=mpegaudio,opus,aac,h264,vp8,vp9,vorbis')
# Theora selects FFmpeg's shared VP3 decoder implementation in configure.
COMPONENTS=dict(DECODER={'PCM_U8','PCM_S16LE','PCM_S24LE','MP3','VORBIS','OPUS','AAC','H264','VP8','VP9','THEORA','VP3'},
    DEMUXER={'WAV','MP3','OGG','MATROSKA','MOV'},PARSER={'MPEGAUDIO','OPUS','AAC','H264','VP8','VP9','VORBIS'},
    PROTOCOL={'FILE','PIPE'},ENCODER=set(),MUXER=set(),INDEV=set(),OUTDEV=set())


def verify_configuration(source):
    """Configure can silently disable requested codecs; inspect actual output."""
    symbols={};hashes={}
    for name in ('config.h','config_components.h'):
        path=source/name
        with path.open('rb') as stream:raw=stream.read(1024*1024+1)
        if len(raw)>1024*1024:raise ValueError('Generated FFmpeg configuration exceeds bound')
        hashes[name]=hashlib.sha256(raw).hexdigest()
        for line in raw.decode('ascii').splitlines():
            parts=line.split()
            if len(parts)==3 and parts[0]=='#define' and parts[1].startswith('CONFIG_') and parts[2] in {'0','1'}:
                if parts[1] in symbols and symbols[parts[1]]!=int(parts[2]):
                    raise ValueError('Conflicting generated FFmpeg configuration')
                symbols[parts[1]]=int(parts[2])
    required={'CONFIG_FFPROBE':1,'CONFIG_SWRESAMPLE':1,'CONFIG_FFMPEG':0,'CONFIG_FFPLAY':0,
        'CONFIG_NETWORK':0,'CONFIG_AVDEVICE':0,'CONFIG_AVFILTER':0}
    for key,value in required.items():
        if symbols.get(key)!=value:raise ValueError('Required FFmpeg configuration differs: '+key)
    for kind,names in COMPONENTS.items():
        expected={'CONFIG_'+name+'_'+kind for name in names}
        enabled={key for key,value in symbols.items() if key.endswith('_'+kind) and value==1}
        if enabled!=expected:
            raise ValueError('FFmpeg '+kind+' capability mismatch: missing='+','.join(sorted(expected-enabled))+
                '; unexpected='+','.join(sorted(enabled-expected)))
    return dict(generated_config_sha256=hashes,required=required,
        components={kind:sorted(names) for kind,names in COMPONENTS.items()},verified=True)


def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for data in iter(lambda:stream.read(1024*1024),b''):digest.update(data)
    return digest.hexdigest()


def extract(archive,destination,expected):
    if not archive.is_file() or archive.stat().st_size>32*1024**2 or sha(archive)!=expected:
        raise ValueError('Reviewed ffprobe source archive hash/size required')
    count=total=0
    with tarfile.open(archive,'r:xz') as source:
        for member in source:
            count+=1;total+=member.size
            path=PurePosixPath(member.name)
            if (count>16384 or total>256*1024**2 or member.size>32*1024**2 or not path.parts
                    or path.parts[0]!='ffmpeg-'+VERSION or path.is_absolute() or '..' in path.parts
                    or len(path.parts)>32 or not (member.isdir() or member.isfile())):
                raise ValueError('Unsupported or excessive ffprobe archive member')
            target=destination.joinpath(*path.parts)
            if member.isdir():target.mkdir(mode=0o700,parents=True,exist_ok=True)
            else:
                target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                with source.extractfile(member) as incoming,target.open('xb') as outgoing:
                    shutil.copyfileobj(incoming,outgoing,1024*1024)
                target.chmod(0o700 if member.mode&0o111 else 0o600)
    if sha(archive)!=expected:raise ValueError('Source archive changed during extraction')
    return destination/('ffmpeg-'+VERSION)


def command(argv,cwd,log,seconds):
    def limits():
        resource.setrlimit(resource.RLIMIT_AS,(1024**3,1024**3))
        resource.setrlimit(resource.RLIMIT_FSIZE,(128*1024**2,128*1024**2))
        resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        resource.setrlimit(resource.RLIMIT_CPU,(seconds,seconds))
    with log.open('xb') as output:
        child=subprocess.Popen(argv,cwd=cwd,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,
            env=dict(PATH='/usr/bin:/bin',HOME=str(cwd),LC_ALL='C',SOURCE_DATE_EPOCH='0'),
            preexec_fn=limits,start_new_session=True)
        try:
            deadline=time.monotonic()+seconds
            while child.poll() is None:
                if time.monotonic()>=deadline:raise TimeoutError('Private build deadline')
                count=total=0
                for directory,folders,files in os.walk(cwd,followlinks=False):
                    for name in [*folders,*files]:
                        entry=Path(directory)/name
                        if entry.is_symlink():raise ValueError('Build produced an unexpected symlink')
                    for name in files:
                        count+=1
                        try:total+=(Path(directory)/name).stat().st_size
                        except FileNotFoundError:continue  # compiler removed its own temporary
                        if count>32768 or total>1024**3:raise ValueError('Private build storage budget exceeded')
                time.sleep(.2)
            result=child.returncode
        except BaseException:
            # Only the fresh build's own process group, never runtime jobs.
            os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
            raise
        if result:raise ValueError('Private ffprobe build failed; see bounded log '+str(log))


def build(archive,output,expected,review_ref):
    if output.exists() or len(expected)!=64 or any(char not in '0123456789abcdef' for char in expected) or not review_ref or len(review_ref)>1024:
        raise ValueError('New private output, exact SHA256 and source review required')
    parent=output.parent
    while not parent.exists():parent=parent.parent
    if shutil.disk_usage(parent).free<2*1024**3:raise ValueError('Private source/build reservation unavailable')
    output.mkdir(mode=0o700,parents=True)
    source=extract(archive,output,expected)
    command(['./configure',*CONFIGURE],source,output/'configure.log',120)
    capabilities=verify_configuration(source)
    (output/'configuration-receipt.json').write_text(json.dumps(capabilities,indent=2)+'\n')
    command(['make','-j1','ffprobe'],source,output/'make.log',240)
    target=output/'ffprobe';shutil.copyfile(source/'ffprobe',target);target.chmod(0o555)
    license=output/'COPYING.LGPLv2.1';shutil.copyfile(source/'COPYING.LGPLv2.1',license);license.chmod(0o444)
    value=dict(format='demiflow_analysis_media_inputs_v1',review_ref=review_ref,tools=[dict(name='ffprobe',version=VERSION,
        path=str(target.resolve()),sha256=sha(target),license_path=str(license.resolve()),license_sha256=sha(license),
        configuration=' '.join(CONFIGURE))])
    (output/'media-inputs.json').write_text(json.dumps(value,indent=2)+'\n')
    (output/'source-receipt.json').write_text(json.dumps(dict(version=VERSION,source_sha256=expected,review_ref=review_ref,
        tool_sha256=sha(target),source_signature_verification='external_review_required',
        configuration_receipt_sha256=sha(output/'configuration-receipt.json'),recipe_sha256=sha(Path(__file__)),
        build_commands=[['./configure',*CONFIGURE],['make','-j1','ffprobe']],image_execution_tested=False,
        isolation_calibrated=False,activation_allowed=False),indent=2)+'\n')
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',required=True,type=Path);parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--source-sha256',required=True);parser.add_argument('--review-ref',required=True)
    args=parser.parse_args();print(json.dumps(build(args.archive,args.output,args.source_sha256,args.review_ref),indent=2))


if __name__=='__main__':main()
