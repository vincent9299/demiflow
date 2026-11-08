"""Exact GPU checks across lengths/layouts, independent of the triage corpus."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import subprocess
import sys


def main(args):
    if args.gpu not in (0,1) or Path(args.output).exists(): raise ValueError('GPU/output ownership')
    directory=Path.home()/'.cache/demiflow/resources'/socket.gethostname()
    directory.mkdir(parents=True,exist_ok=True)
    with (directory/f'gpu_{args.gpu}.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if subprocess.check_output(['nvidia-smi',f'--id={args.gpu}','--query-compute-apps=pid','--format=csv,noheader']).strip():
            raise RuntimeError('GPU occupied')
        os.environ['CUDA_VISIBLE_DEVICES']=str(args.gpu)
        os.environ.setdefault('TRITON_CACHE_DIR',str(Path(args.output).parent/'triton-cache'))
        import torch
        torch.set_num_threads(2)
        torch.cuda.set_per_process_memory_fraction(512*1024**2/torch.cuda.get_device_properties(0).total_memory)
        sys.path.append(str(Path(__file__).resolve().parents[1]/'demiflow/services'))
        import pointwise_cuda
        import transformers.models.qwen3_5.modeling_qwen3_5 as qwen
        convolution=pointwise_cuda.convolution(qwen.causal_conv1d_fn,qwen)
        torch.manual_seed(1729)
        report=dict(convolutions=0,normalizations=0,maximum_length=0)
        with torch.inference_mode():
            for length in (1,2,3,17,127,305,1024,8192):
                for channels in (7,128,5120):
                    for batch in (1,2):
                        if batch*channels*length>1_000_000: continue
                        for channel_last in (False,True):
                            shape=(batch,length,channels) if channel_last else (batch,channels,length)
                            x=torch.randn(shape,device='cuda',dtype=torch.bfloat16)
                            if channel_last:x=x.transpose(1,2)
                            for width in (2,3,4):
                                weight=torch.randn(channels,width,device='cuda',dtype=torch.bfloat16)
                                for biased in (False,True):
                                    bias=torch.randn(channels,device='cuda',dtype=torch.bfloat16) if biased else None
                                    native=qwen.causal_conv1d_fn(x,weight,bias,activation='silu')
                                    candidate=convolution(x,weight,bias,activation='silu')
                                    if not torch.equal(native,candidate):
                                        raise AssertionError(f'Convolution differs: {batch,channels,length,width,channel_last,biased}')
                                    report['convolutions']+=1
                                    del native,candidate,bias
                                del weight
                            del x
                        report['maximum_length']=max(report['maximum_length'],length)
                    if channels*length>1_000_000:continue
                    for gated in (False,True):
                        cls=qwen.Qwen3_5RMSNormGated if gated else qwen.Qwen3_5RMSNorm
                        layer=cls(channels).to(device='cuda',dtype=torch.bfloat16)
                        layer.weight.copy_(torch.randn_like(layer.weight))
                        optimized=pointwise_cuda.normalization(layer,layer.forward,gated=gated)
                        for scale in (0.,.0001,1.,100.):
                            x=torch.randn((1,length,channels),device='cuda',dtype=torch.bfloat16)*scale
                            gate=torch.randn_like(x)
                            values=(x,gate) if gated else (x,)
                            native=layer(*values);candidate=optimized(*values)
                            if not torch.equal(native,candidate):
                                raise AssertionError(f'Normalization differs: {channels,length,gated,scale}')
                            report['normalizations']+=1
                            del x,gate,native,candidate,values
                        del layer,optimized
                torch.cuda.empty_cache()
        report.update(passed=True,peak_reserved_bytes=torch.cuda.max_memory_reserved())
        Path(args.output).write_text(json.dumps(report,indent=2));print(json.dumps(report))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gpu',type=int,required=True);parser.add_argument('--output',required=True)
    main(parser.parse_args())
