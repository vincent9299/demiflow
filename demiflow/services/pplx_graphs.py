"""Bounded exact-shape CUDA graphs for the native pplx reference path.

No Torch import at module import/configuration time. This owns GPU execution
only; HTTP admission, row scheduling, retries and journals remain unchanged.
"""
import copy
import time

try:
    from .cuda_graphs import (MiB, DEFAULTS, graph_config, check_encoding_budget, configure_allocator,
                              GraphCache, ExecutionGate, BoundedExecution, qwen_workspace_bytes, graph_pool_bytes)
except ImportError:
    from cuda_graphs import (MiB, DEFAULTS, graph_config, check_encoding_budget, configure_allocator,
                             GraphCache, ExecutionGate, BoundedExecution, qwen_workspace_bytes, graph_pool_bytes)


class _Entry:
    def close(self):
        self.graph.reset()
        self.graph = self.ids = self.cols = self.output = self.keepalive = None


class PplxGraphAdapter(BoundedExecution):
    """Native decide with isolated mutable state, shared parameters and bounded graphs."""
    def __init__(self, decider, configuration):
        import torch
        try:
            from .model_adapters import validate_request
        except ImportError:
            from model_adapters import validate_request
        if decider.fast is not None or str(decider.backbone.dtype) != 'torch.bfloat16':
            raise ValueError('Graphs require the native bf16 reference model')
        self.validate, self.decider = validate_request, decider
        self._initialize(configuration, lambda tokens: qwen_workspace_bytes(decider.backbone.config.text_config, tokens),
                         'pplx_cuda_graphs')
        for index in range(self.parallel):
            lane = copy.copy(decider)
            lane.backbone = copy.deepcopy(decider.backbone, {id(p): p for p in decider.backbone.parameters()})
            if not all(a.data_ptr() == b.data_ptr() for a, b in zip(
                    decider.backbone.parameters(), lane.backbone.parameters(), strict=True)):
                raise RuntimeError('Model clone did not share weights')
            native = lane.reference_logits
            lane.reference_logits = lambda chunks, index=index: self._forward(index, chunks)
            self._add_lane(lane, native=native)
        self._finish_init()

    def single(self, request):
        request = self.validate(request)
        check_encoding_budget(request, self.config['max_input_tokens'])
        self._healthy()
        if not self.accepting: raise RuntimeError('Graph execution is closing')
        with self.encoding:
            plan = self.decider.encoder.plan(request, self.decider.max_length)
        if not plan or sum(chunk.length for chunk in plan) > self.config['max_input_tokens']:
            raise ValueError('Request exceeds cuda_graphs.max_input_tokens; input was not truncated')
        shape = (plan[0].length, len(plan[0].keys)) if len(plan) == 1 else None
        return self._submit(request, plan, sum(chunk.length for chunk in plan), shape)

    def _execute(self, index, request, plan):
        lane = self.lanes[index]
        encoder = self.decider.encoder
        # Preserve native decide/_run/_batches/_respond, without encoding twice.
        class PreparedEncoder:
            def __getattr__(self, name): return getattr(encoder, name)
            def plan(self, value, limit):
                if value is not request: raise RuntimeError('Native request identity changed')
                return plan
        lane['model'].encoder = PreparedEncoder()
        try:
            with self.torch.inference_mode(), self.torch.cuda.stream(lane['stream']):
                return lane['model'].decide(request)
        finally:
            lane['model'].encoder = encoder

    def _native(self, lane, chunks):
        self.stats['native'] += 1
        result = lane['native'](chunks)
        lane['stream'].synchronize()
        return result

    def _replay(self, entry, chunk, stream):
        entry.ids.copy_(chunk.input_ids.unsqueeze(0))
        entry.cols.copy_(chunk.markers)
        entry.graph.replay()
        logits = entry.output.float().cpu()
        stream.synchronize()
        return [[logits[i, :count].tolist() for i, count in enumerate(chunk.counts)]]

    def _forward(self, index, chunks):
        lane = self.lanes[index]
        eligible = (len(chunks) == 1 and chunks[0].pixel_values is None
                    and chunks[0].image_grid_thw is None
                    and (chunks[0].mm_token_type_ids is None
                         or not bool(chunks[0].mm_token_type_ids.any()))
                    and chunks[0].length <= self.config['max_graph_tokens'])
        if not eligible:
            with self.gate.enter(exclusive=True):
                self._healthy()
                return self._native(lane, chunks)
        chunk = chunks[0]
        shape = (chunk.length, len(chunk.keys))
        key = (index, shape)
        hot = self.cache.observe(key)
        audit_failed = False
        with self.gate.enter():
            self._healthy()
            if self.cache.disabled:
                return self._native(lane, chunks)
            entry = self.cache.get(key)
            if entry is not None:
                entry.uses += 1
                if entry.uses % self.config['verify_every']:
                    self.stats['hits'] += 1
                    return self._replay(entry, chunk, lane['stream'])
                # Existing graphs and native forwards only read shared weights.
                # Keep the exact audit on this lane without draining other lanes.
                expected = self._native(lane, chunks)
                actual = self._replay(entry, chunk, lane['stream'])
                self.stats['checks'] += 1
                if actual == expected:
                    self.stats['hits'] += 1
                    return actual
                self.stats['mismatches'] += 1
                self.cache.disabled = True
                audit_failed = True
            elif self.cache.disabled or not hot or not self.cache.admit(self._estimate(chunk.length), key):
                return self._native(lane, chunks)
            else:
                # The independent reference is ordinary lane-local execution.
                # Prepare it while other lanes can still replay; only capture
                # and pool mutation need to drain them.
                expected = self._native(lane, chunks)
        # Capture and freeing private pools still drain every active GPU reader.
        with self.gate.enter(exclusive=True):
            self._healthy()
            if audit_failed:
                self.cache.clear()
                self.torch.cuda.empty_cache()
                return expected
            if self.cache.disabled:
                return expected
            entry = self.cache.get(key)
            if entry is None and self.cache.admit(self._estimate(chunk.length), key):
                entry = self._capture(index, chunk)
            if entry is None:
                return expected
            actual = self._replay(entry, chunk, lane['stream'])
            self.stats['checks'] += 1
            if actual != expected:
                self.stats['mismatches'] += 1
                self.cache.disabled = True
                self.cache.clear()
                self.torch.cuda.empty_cache()
                return expected
            self.stats['hits'] += 1
            return actual

    def _capture(self, index, chunk):
        torch, lane = self.torch, self.lanes[index]
        reserve = self._estimate(chunk.length)
        self.cache.make_room(reserve)
        torch.cuda.empty_cache()
        self.cache.captures += 1
        start = time.monotonic()
        entry = _Entry()
        entry.graph = None
        try:
            ids = chunk.input_ids.unsqueeze(0).to('cuda')
            cols = chunk.markers.to('cuda')
            rows = torch.zeros(len(chunk.keys), dtype=torch.long, device='cuda')
            mask = dict(full_attention=torch.ones((1, 1, 1, chunk.length), dtype=torch.bool, device='cuda'),
                        linear_attention=None)
            def forward():
                if __package__:
                    from .capture_kernels import capture_kernels
                else:
                    from capture_kernels import capture_kernels
                with capture_kernels(lane['model'].backbone, self.gate):
                    hidden = lane['model'].backbone(input_ids=ids, attention_mask=mask, use_cache=False).last_hidden_state
                    return lane['model'].readout(hidden[rows, cols])
            # The shape has already run natively. Warm these exact static
            # buffers once before capture; repeating three forwards only adds
            # time under the global GPU exclusion gate.
            forward()
            lane['stream'].synchronize()
            # Torch 2.14 caches cuBLAS workspaces by handle/stream. Resetting
            # any graph on that stream clears them, invalidating other graphs
            # if they captured an eager workspace. Allocate each workspace
            # inside its graph's private pool, then remove the external cache
            # reference. The pool keeps its addresses alive until eviction.
            torch._C._cuda_clearCublasWorkspaces()
            torch.cuda.empty_cache()
            # A fresh private pool cannot grow beyond its pre-reserved budget.
            cap = min(self.memory_limit, torch.cuda.memory_reserved() + reserve)
            torch.cuda.set_per_process_memory_fraction(cap / self.total_memory, 0)
            graph = torch.cuda.CUDAGraph()
            entry.graph = graph
            with torch.cuda.graph(graph, stream=lane['stream']): output = forward()
            lane['stream'].synchronize()
            torch._C._cuda_clearCublasWorkspaces()
            pool_bytes = graph_pool_bytes(torch, graph)
            static_bytes = ids.nbytes + cols.nbytes + rows.nbytes + mask['full_attention'].nbytes
            entry.bytes = pool_bytes + static_bytes
            entry.ids, entry.cols, entry.output = ids, cols, output
            entry.keepalive, entry.uses = (rows, mask), 0
            if entry.bytes > reserve:
                entry.close()
                self.stats['capture_rejected'] += 1
                return None
            self.cache.put((index, (chunk.length, len(chunk.keys))), entry)
            return entry
        except BaseException as error:
            # Do not continue GPU work after an unknown capture/CUDA error.
            self.failed = 'CUDA graph capture failed: ' + str(error)[:512]
            with self.condition: self.condition.notify_all()
            raise
        finally:
            torch.cuda.set_per_process_memory_fraction(self.memory_limit / self.total_memory, 0)
            self.cache.capture_seconds += time.monotonic() - start
            self.stats['reserved_peak_bytes'] = max(self.stats['reserved_peak_bytes'], torch.cuda.memory_reserved())

