"""Capture ownership and reference restoration; no Torch import required."""
import sys
from types import SimpleNamespace

import pytest

from demiflow.services.capture_kernels import capture_kernels, _torch_convolution
from demiflow.services.cuda_graphs import ExecutionGate


def fixture(monkeypatch, *, fail_install=False):
    class Norm:
        def forward(self, x): return x*2
    class Gated:
        def forward(self, hidden_states, gate): return hidden_states*gate
    class Conv: pass
    torch_function=lambda x:x*3
    implementation=torch_function
    is_new_implementation=False
    def reference(x):
        if is_new_implementation: return implementation(x)
        return torch_function(x)
    qwen=SimpleNamespace(Qwen3_5RMSNorm=Norm,Qwen3_5RMSNormGated=Gated,
        Qwen3_5GatedDeltaNet=Conv,causal_conv1d_fn=reference)
    plain, gated=Norm(),Gated()
    def normalization(module, original, *, gated):
        if gated and fail_install: raise ValueError('install failed')
        return lambda *a,**k:original(*a,**k)+1
    kernels=SimpleNamespace(normalization=normalization,
                            convolution=lambda original,qwen:lambda x:original(x)+1)
    monkeypatch.setitem(sys.modules,'transformers.models.qwen3_5.modeling_qwen3_5',qwen)
    monkeypatch.setitem(sys.modules,'demiflow.services.pointwise_cuda',kernels)
    model=SimpleNamespace(modules=lambda:iter([plain,gated,Conv()]))
    return model,qwen,plain,gated,reference


@pytest.mark.parametrize('fail_inside',[False,True])
def test_capture_restores_native_functions_and_instance_bindings(monkeypatch,fail_inside):
    model,qwen,plain,gated,reference=fixture(monkeypatch)
    gate=ExecutionGate()
    assert _torch_convolution(reference)
    with gate.enter(exclusive=True):
        try:
            with capture_kernels(model,gate):
                assert plain.forward(2)==5 and gated.forward(2,3)==7
                assert qwen.causal_conv1d_fn(2)==7
                if fail_inside: raise ValueError('capture failed')
        except ValueError:
            assert fail_inside
    assert plain.forward(2)==4 and gated.forward(2,3)==6
    assert 'forward' not in plain.__dict__ and 'forward' not in gated.__dict__
    assert qwen.causal_conv1d_fn is reference


def test_partial_install_failure_restores_every_prior_override(monkeypatch):
    model,qwen,plain,gated,reference=fixture(monkeypatch,fail_install=True)
    gate=ExecutionGate()
    with gate.enter(exclusive=True), pytest.raises(ValueError,match='install failed'):
        with capture_kernels(model,gate): pass
    assert plain.forward(2)==4 and 'forward' not in plain.__dict__
    assert gated.forward(2,3)==6 and qwen.causal_conv1d_fn is reference


def test_native_custom_forward_and_unknown_convolution_are_preserved(monkeypatch):
    model,qwen,plain,gated,_=fixture(monkeypatch)
    own=lambda x:x+7
    plain.forward=own
    unknown=lambda x:x-1
    qwen.causal_conv1d_fn=unknown
    gate=ExecutionGate()
    with gate.enter(exclusive=True), capture_kernels(model,gate):
        assert qwen.causal_conv1d_fn is unknown
        assert plain.forward(2)==10
    assert plain.forward is own and qwen.causal_conv1d_fn is unknown


def test_capture_requires_drained_gpu_readers(monkeypatch):
    model,_,_,_,_=fixture(monkeypatch)
    gate=ExecutionGate()
    with gate.enter(), pytest.raises(RuntimeError,match='exclusive GPU ownership'):
        with capture_kernels(model,gate): pass
