"""Version-scoped vLLM/JEV state ownership; imported only by the owned worker.

The native runner is called under the execution gate. Captured forwards own
all mutable state and only share weights. No installed package is edited.
"""
import copy
import dataclasses
import hashlib
import importlib.metadata
import os
from pathlib import Path

MiB = 1024 * 1024
SUPPORTED = {'vllm': '0.31.0', 'torch': '2.13.0', 'startlux-decide': '1.1.0',
             'flashinfer-python': '0.7.0.post1', 'nvidia-cutlass-dsl': '4.7.1'}
SCRATCH_SOURCES = {
    'delta_rule_sm90': 'ee4c776ee3b01ac47e7146f6fc092f9b8b4004a4e4b39cb38335cc525ccc278c',
    'delta_rule_cp_sm90': '618c4e0124dd381c1c0c15fba870b73eccc81b5f2f1af8f0ede12d725f04b484',
}


def check_runtime():
    for name, required in SUPPORTED.items():
        if importlib.metadata.version(name).split('+')[0] != required:
            raise ValueError(f'JEV graph backend requires {name}=={required}; use the native profile otherwise')


def isolate_gdn_scratch():
    """Fix only the reviewed SM90 GDN imports in this dedicated process."""
    import torch
    from flashinfer.gdn_kernels.delta_rule_dsl import delta_rule_sm90, delta_rule_cp_sm90
    modules = [delta_rule_sm90, delta_rule_cp_sm90]
    for module in modules:
        path = Path(module.__file__)
        with path.open('rb') as stream: source = stream.read(MiB + 1)
        if len(source) > MiB or hashlib.sha256(source).hexdigest() != SCRATCH_SOURCES[module.__name__.rsplit('.', 1)[-1]]:
            raise ValueError('Unreviewed FlashInfer GDN source; refusing to apply the workspace fix')
    original = [(module, module._get_cache_buf) for module in modules]
    def allocate(name, num_bytes, device, zero_init=False):
        if type(num_bytes) is not int or not 0 <= num_bytes <= 256 * MiB:
            raise ValueError('JEV GDN scratch exceeds the reviewed 256 MiB allocation bound')
        return (torch.zeros if zero_init else torch.empty)(num_bytes, dtype=torch.uint8, device=device)
    for module in modules: module._get_cache_buf = allocate
    def restore():
        for module, value in original: module._get_cache_buf = value
    return restore


class TensorClone:
    """Clone reviewed metadata only, charging tensors before materialization."""
    def __init__(self, torch, budget=128 * MiB):
        self.torch, self.budget, self.bytes, self.memo = torch, budget, 0, {}

    def __call__(self, value):
        if value is None or type(value) in (bool, int, float, str): return value
        if id(value) in self.memo: return self.memo[id(value)]
        if isinstance(value, self.torch.Tensor):
            self.bytes += value.numel() * value.element_size()
            if self.bytes > self.budget: raise ValueError('JEV metadata/input exceeds its copy budget')
            result = value.clone()
        elif isinstance(value, dict): result = {k: self(v) for k, v in value.items()}
        elif isinstance(value, list): result = [self(v) for v in value]
        elif isinstance(value, tuple): result = tuple(self(v) for v in value)
        elif type(value).__module__ == 'startlux_decide.vllm_plugin' and type(value).__name__ == 'Plan':
            result = copy.copy(value)
            for name in value.__slots__: setattr(result, name, self(getattr(value, name)))
        elif dataclasses.is_dataclass(value):
            result = copy.copy(value)
            for name, item in vars(value).items(): setattr(result, name, self(item))
        else: raise ValueError('Unreviewed JEV state type: ' + type(value).__name__)
        self.memo[id(value)] = result
        return result


def clone_context(context, clone):
    result = copy.copy(context)
    for name in ('attn_metadata', 'slot_mapping', 'additional_kwargs', 'is_padding'):
        setattr(result, name, clone(getattr(context, name)))
    return result


class LaneState:
    """Long-lived mutable model state shared only by graphs on one fixed lane."""
    def __init__(self, torch, layers, max_tokens, budget=1024 * MiB):
        self.layers, self.bytes = {}, 0
        plans, sizes = [], set()
        for name, layer in layers.items():
            cache = getattr(layer, 'kv_cache', None)
            if isinstance(cache, tuple):
                views = tuple(t[:2] for t in cache)
            elif isinstance(cache, torch.Tensor):
                if cache.ndim != 4 or cache.shape[1] != 4 or cache.shape[3] != 512:
                    raise ValueError('Unreviewed JEV attention cache layout')
                sizes.add(cache.shape[2])
                blocks = (max_tokens + cache.shape[2] - 1) // cache.shape[2] + 1
                if blocks > cache.shape[0]: raise ValueError('Native JEV cache cannot cover the configured graph length')
                views = cache[:blocks]
            elif cache is None: views = None
            else: raise ValueError('Unreviewed JEV state cache')
            self.bytes += sum(t.numel() * t.element_size() for t in views) if isinstance(views, tuple) else (views.numel() * views.element_size() if views is not None else 0)
            if self.bytes > budget: raise ValueError('JEV lane state exceeds 1 GiB; lower max_graph_tokens')
            plans.append((name, layer, views))
        if len(sizes) != 1: raise ValueError('JEV requires one reviewed attention block size')
        self.block_size = sizes.pop()
        # All layer sizes have been checked before allocating the first clone.
        for name, layer, views in plans:
            new = copy.copy(layer)
            if isinstance(layer, torch.nn.Module):
                new._buffers, new._modules, new._parameters = dict(layer._buffers), dict(layer._modules), dict(layer._parameters)
            if isinstance(views, tuple): new.kv_cache = tuple(torch.zeros_like(t) for t in views)
            elif views is not None: new.kv_cache = torch.zeros_like(views)
            self.layers[name] = new

    def context(self, original, torch, tokens, clone):
        from vllm.config import CUDAGraphMode
        if original.cudagraph_runtime_mode not in (CUDAGraphMode.NONE, CUDAGraphMode.PIECEWISE):
            raise ValueError('Unreviewed JEV execution mode')
        ctx = clone_context(original, clone)
        if ctx.no_compile_layers.keys() != self.layers.keys(): raise ValueError('JEV layer registry changed')
        ctx.no_compile_layers = self.layers
        ctx.cudagraph_runtime_mode = CUDAGraphMode.NONE
        blocks = (tokens + self.block_size - 1) // self.block_size
        for meta in ctx.attn_metadata.values():
            for name in ('non_spec_state_indices_tensor', 'prefill_state_indices'):
                value = getattr(meta, name, None)
                if value is not None: value.fill_(1)  # block zero is native NULL_BLOCK_ID
            value = getattr(meta, 'block_table', None)
            if value is not None:
                value.zero_()
                value[0, :blocks] = torch.arange(1, blocks + 1, device=value.device)
            for name in ('has_initial_state', 'prefill_has_initial_state'):
                value = getattr(meta, name, None)
                if value is not None and value.any(): raise ValueError('JEV graph requires a fresh one-pass request')
            value = getattr(meta, 'slot_mapping', None)
            if value is not None: self._slots(value, torch)
        for value in ctx.slot_mapping.values(): self._slots(value, torch)
        return ctx

    def _slots(self, value, torch):
        value.copy_(torch.where(value >= 0, torch.arange(value.numel(), device=value.device) + self.block_size, -1))


def release_lane_workspace(torch, stream):
    """Reset only this lane's cuBLAS bindings, preserving native vLLM graphs.

    In the pinned Torch, CUDAGraph.reset calls clearCublasWorkspacesForStream.
    A one-element housekeeping capture uses that scoped lifecycle operation;
    the process-wide _cuda_clearCublasWorkspaces would invalidate native graphs.
    Caller holds the exclusive GPU gate. Existing graph pools retain their own
    workspace allocations after their handle bindings are released.
    """
    control = torch.empty((), device='cuda')
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph, stream=stream): control.zero_()
    graph.reset()


class JEVRuntime:
    def __init__(self, path, options):
        check_runtime()
        os.environ.update(VLLM_ENABLE_V1_MULTIPROCESSING='0', STARTLUX_PREFIX_BIDIR='1', VLLM_USE_FLASHINFER_SAMPLER='0')
        import torch
        if torch.version.git_version != 'cf30153c4c131c8164ee7798e5022d810682e2cb':
            raise ValueError('JEV graph workspace lifecycle requires the reviewed Torch build; use native otherwise')
        if torch.cuda.device_count() != 1 or torch.cuda.get_device_capability(0)[0] != 9:
            raise ValueError('The verified JEV graph backend requires one SM90 GPU')
        self.torch = torch
        self.restore_scratch = isolate_gdn_scratch()
        self.llm = None
        try:
            from vllm import LLM
            self.llm = LLM(model=str(path), dtype=options.get('dtype', 'bfloat16'),
                max_model_len=options.get('max_model_len', 32768),
                max_num_batched_tokens=options.get('max_num_batched_tokens', options.get('max_model_len', 32768)),
                max_num_seqs=options.get('max_num_seqs', 64), gpu_memory_utilization=options.get('gpu_memory_utilization', .85),
                enable_prefix_caching=False, enable_chunked_prefill=False, logprobs_mode='processed_logprobs',
                max_logprobs=255, attention_config={'backend': 'FLASH_ATTN'}, generation_config='vllm',
                limit_mm_per_prompt={'image': 0, 'video': 0})
            self.worker = self.llm.llm_engine.model_executor.driver_worker.worker
            self.model = self.worker.get_model()
            self.layers = self.worker.model_runner.compilation_config.static_forward_context
            self.config = self.worker.model_runner.model_config.hf_text_config
        except BaseException:
            self.close()
            raise

    def native(self, prompt, token_ids, allowed_ids, *, snapshot=False):
        from vllm import SamplingParams
        from vllm.forward_context import get_forward_context
        torch, captured = self.torch, []
        def hook(module, args, kwargs, output):
            if captured: raise RuntimeError('JEV produced more than one forward for a single request')
            tokens = len(token_ids)
            if not tokens <= output.shape[0] <= max(tokens, 128): raise ValueError('Unexpected native JEV padding')
            clone = TensorClone(torch)
            inputs = clone(kwargs)
            context = clone_context(get_forward_context(), clone)
            reference = clone(output[:tokens].detach())
            captured.append((inputs, context, reference))
        handle = self.model.register_forward_hook(hook, with_kwargs=True) if snapshot else None
        try:
            response = self.llm.generate(prompt, SamplingParams(max_tokens=1, temperature=1,
                logprobs=len(allowed_ids), allowed_token_ids=allowed_ids), use_tqdm=False)[0]
            if response.prompt_token_ids != token_ids or len(response.outputs[0].token_ids) != 1:
                raise RuntimeError('Native JEV tokenization or usage differs from the prepared request')
            scores = [response.outputs[0].logprobs[0][t].logprob for t in allowed_ids]
            torch.cuda.synchronize()
            if snapshot and len(captured) != 1: raise RuntimeError('Native JEV did not provide its reference forward')
            return scores, captured[0] if snapshot else None
        finally:
            if handle is not None: handle.remove()

    def close(self):
        try:
            if self.llm is not None: self.llm.llm_engine.engine_core.shutdown()
        finally: self.restore_scratch()
