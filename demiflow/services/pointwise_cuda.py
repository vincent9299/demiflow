"""Inference-only pointwise kernels for graph capture, preserving native rounding.

Reductions and gated SiLU remain PyTorch operations. Input length/strides do not
specialize the JIT cache; only fixed model widths, epsilon and kernel kind do.
Import only inside an owned model worker with Torch and Triton already present.
"""
import torch
import triton
import triton.language as tl


@triton.jit(do_not_specialize=['N', 'T', 'SB', 'SD', 'ST'])
def _conv(X, W, Y, N, D: tl.constexpr, T, K: tl.constexpr,
          SB, SD, ST,
          SINGLE: tl.constexpr, BLOCK: tl.constexpr):
    i = tl.program_id(0)*BLOCK + tl.arange(0, BLOCK)
    channel = i % D
    time = i // D
    batch = tl.full((BLOCK,), 0, tl.int32)
    if not SINGLE:
        batch = time // T
        time = time % T
    acc = tl.full((BLOCK,), 0, tl.float32)
    for k in range(K):
        source_time = time-(K-1)+k
        x = tl.load(X+batch*SB+channel*SD+source_time*ST, (i<N)&(source_time>=0), 0).to(tl.float32)
        weight = tl.load(W+channel*K+k, i<N, 0).to(tl.float32)
        acc = acc + x*weight
    tl.store(Y+i, acc, i<N)


def convolution(reference, qwen):
    def apply(x, weight, bias=None, activation=None, **kwargs):
        # Bias changes native convolution's accumulation/rounding convention.
        # Qwen3.5 uses bias-free convolution; other variants keep their reference.
        if (not x.is_cuda or x.dtype != torch.bfloat16 or weight.dtype != x.dtype or weight.device != x.device
                or x.ndim != 3 or x.numel() == 0 or x.numel() >= 2**31 or weight.ndim != 2 or weight.shape[0] != x.shape[1]
                or weight.shape[1] not in (2, 3, 4) or not weight.is_contiguous()
                or bias is not None):
            return reference(x, weight, bias, activation=activation, **kwargs)
        batch, channels, length = x.shape
        output = torch.empty((batch, length, channels), dtype=x.dtype, device=x.device)
        _conv[(triton.cdiv(x.numel(), 256),)](x, weight,
            output, x.numel(), channels, length, weight.shape[1], *x.stride(),
            batch == 1, 256, enable_fp_fusion=False)
        output = output.transpose(1, 2)
        if activation is not None: output = qwen.ACT2FN[activation](output)
        return output
    return apply


@triton.jit(do_not_specialize=['N'])
def _finish(X, V, W, G, Y, N, D: tl.constexpr,
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



def normalization(module, original, *, gated):
    def eligible(x):
        return (x.is_cuda and x.ndim>0 and x.dtype == torch.bfloat16 and x.is_contiguous()
                and 0 < x.numel() < 2**31 and module.weight.dtype == torch.bfloat16
                and module.weight.is_contiguous() and module.weight.device == x.device
                and module.weight.shape == (x.shape[-1],))

    def plain(x):
        if not eligible(x): return original(x)
        variance = x.float().pow(2).mean(-1, keepdim=True)
        output = torch.empty_like(x)
        _finish[(triton.cdiv(x.numel(), 1024),)](x, variance, module.weight, x, output,
            x.numel(), x.shape[-1], module.eps, False, 1024, enable_fp_fusion=False)
        return output

    def gate(hidden_states, gate):
        x = hidden_states
        if (not eligible(x) or not gate.is_contiguous() or gate.shape != x.shape or gate.device != x.device
                or module.activation != 'silu'):
            return original(x, gate)
        variance = x.float().pow(2).mean(-1, keepdim=True)
        activation = torch.nn.functional.silu(gate.to(torch.float32))
        output = torch.empty_like(x)
        _finish[(triton.cdiv(x.numel(), 1024),)](x, variance, module.weight, activation, output,
            x.numel(), x.shape[-1], module.variance_epsilon, True, 1024, enable_fp_fusion=False)
        return output
    return gate if gated else plain
