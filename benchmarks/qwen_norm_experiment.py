"""Owned-process experiment: fuse only pointwise RMSNorm work.

Keep the original PyTorch float32 square/mean reduction and gated SiLU. Every
native BF16 rounding boundary remains explicit. No production import or patch.
"""
import torch
import triton
import triton.language as tl


@triton.jit
def _conv(X, W, BIAS, Y, N, D: tl.constexpr, T: tl.constexpr, K: tl.constexpr,
          SB: tl.constexpr, SD: tl.constexpr, ST: tl.constexpr,
          BIASED: tl.constexpr, BLOCK: tl.constexpr):
    i = tl.program_id(0)*BLOCK + tl.arange(0, BLOCK)
    channel = i % D
    time = (i // D) % T
    batch = i // (D*T)
    acc = tl.full((BLOCK,), 0, tl.float32)
    if BIASED: acc = tl.load(BIAS+channel, i<N, 0).to(tl.float32)
    for k in range(K):
        source_time = time-(K-1)+k
        x = tl.load(X+batch*SB+channel*SD+source_time*ST, (i<N)&(source_time>=0), 0).to(tl.float32)
        weight = tl.load(W+channel*K+k, i<N, 0).to(tl.float32)
        acc = acc + x*weight
    tl.store(Y+i, acc, i<N)


def convolution(reference, qwen):
    def apply(x, weight, bias=None, activation=None, **kwargs):
        if (not x.is_cuda or x.dtype != torch.bfloat16 or weight.dtype != x.dtype
                or x.ndim != 3 or weight.ndim != 2 or weight.shape[0] != x.shape[1]
                or weight.shape[1] not in (2, 3, 4) or not weight.is_contiguous()
                or bias is not None and (bias.dtype != x.dtype or not bias.is_contiguous())):
            return reference(x, weight, bias, activation=activation, **kwargs)
        batch, channels, length = x.shape
        output = torch.empty((batch, length, channels), dtype=x.dtype, device=x.device)
        _conv[(triton.cdiv(x.numel(), 256),)](x, weight, bias if bias is not None else x,
            output, x.numel(), channels, length, weight.shape[1], *x.stride(),
            bias is not None, 256, enable_fp_fusion=False)
        output = output.transpose(1, 2)
        if activation is not None: output = qwen.ACT2FN[activation](output)
        return output
    return apply


@triton.jit
def _finish(X, V, W, G, Y, N: tl.constexpr, D: tl.constexpr,
            EPS: tl.constexpr, GATED: tl.constexpr, BLOCK: tl.constexpr):
    i = tl.program_id(0) * BLOCK + tl.arange(0, BLOCK)
    x = tl.load(X+i, i<N, 0).to(tl.float32)
    variance = tl.load(V+i//D, i<N, 0)
    w = tl.load(W+i%D, i<N, 0).to(tl.float32)
    normalized = x * tl.rsqrt(variance + EPS)
    if GATED:
        # Native casts normalization before BF16 weight multiplication, then
        # casts that product before multiplying the float32 SiLU gate.
        normalized = normalized.to(tl.bfloat16).to(tl.float32)
        weighted = (normalized*w).to(tl.bfloat16).to(tl.float32)
        result = weighted * tl.load(G+i, i<N, 0)
    else:
        result = normalized * (1.0+w)
    tl.store(Y+i, result, i<N)


def install(qwen):
    original = qwen.Qwen3_5RMSNorm.forward
    original_gated = qwen.Qwen3_5RMSNormGated.forward

    def eligible(x, weight):
        return (x.is_cuda and x.dtype == torch.bfloat16 and x.is_contiguous()
                and weight.dtype == torch.bfloat16 and weight.is_contiguous())

    def plain(self, x):
        if not eligible(x, self.weight): return original(self, x)
        variance = x.float().pow(2).mean(-1, keepdim=True)
        output = torch.empty_like(x)
        _finish[(triton.cdiv(x.numel(), 1024),)](x, variance, self.weight, x, output,
            x.numel(), x.shape[-1], self.eps, False, 1024, enable_fp_fusion=False)
        return output

    def gated(self, x, gate):
        if not eligible(x, self.weight) or not gate.is_contiguous():
            return original_gated(self, x, gate)
        variance = x.float().pow(2).mean(-1, keepdim=True)
        activation = qwen.ACT2FN[self.activation](gate.to(torch.float32))
        output = torch.empty_like(x)
        _finish[(triton.cdiv(x.numel(), 1024),)](x, variance, self.weight, activation, output,
            x.numel(), x.shape[-1], self.variance_epsilon, True, 1024, enable_fp_fusion=False)
        return output

    qwen.Qwen3_5RMSNorm.forward = plain
    qwen.Qwen3_5RMSNormGated.forward = gated
    def restore():
        qwen.Qwen3_5RMSNorm.forward = original
        qwen.Qwen3_5RMSNormGated.forward = original_gated
    return restore
