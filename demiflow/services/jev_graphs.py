"""Independent JEV forwards using the shared bounded execution/cache policy."""
from concurrent.futures import ThreadPoolExecutor
import time

try:
    from .cuda_graphs import BoundedExecution, MiB, check_encoding_budget, graph_pool_bytes
    from .jev_runtime import JEVRuntime, LaneState, TensorClone, release_lane_workspace
except ImportError:
    from cuda_graphs import BoundedExecution, MiB, check_encoding_budget, graph_pool_bytes
    from jev_runtime import JEVRuntime, LaneState, TensorClone, release_lane_workspace


class JEVEntry:
    def close(self):
        self.graph.reset()
        self.graph = self.context = self.inputs = self.hidden = self.scores = None
        self.allowed = self.row = self.read_ids = None


class JEVGraphAdapter(BoundedExecution):
    def __init__(self, protocol, path, options, configuration):
        self.protocol = protocol
        self.native_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='jev-native')
        self.runtime = None
        try:
            self.runtime = self.native_executor.submit(JEVRuntime, path, options).result()
            self._initialize(configuration, self._workspace, 'jev_cuda_graphs')
            for _ in range(self.parallel):
                state = LaneState(self.torch, self.runtime.layers, self.config['max_graph_tokens'])
                self._add_lane(self.runtime.model, state=state)
            self._finish_init()
        except BaseException:
            if hasattr(self, 'lanes'):
                for lane in self.lanes: lane['executor'].shutdown(wait=True)
            try:
                if self.runtime is not None:
                    self.native_executor.submit(self.runtime.close).result()
            finally: self.native_executor.shutdown(wait=True)
            raise

    def _workspace(self, tokens):
        # FlashAttention avoids a materialized attention matrix. Account for
        # GDN intermediates, projections and the native readout, with allocator
        # enforcement as the final guard for third-party allocation variation.
        config = self.runtime.config
        width = (config.hidden_size * 64 + config.intermediate_size * 16
                 + config.linear_num_value_heads * config.linear_value_head_dim * 32)
        return 64 * MiB + tokens * width

    def single(self, request):
        questions = request.get('questions') if isinstance(request, dict) else None
        if not isinstance(questions, dict) or not 1 <= len(questions) <= 128:
            raise ValueError('Expected 1..128 questions')
        check_encoding_budget(request, self.config['max_input_tokens'])
        self._healthy()
        plan = self.protocol._prepare(request, tokens=True)
        total = sum(item[4] for item in plan)
        if total > self.config['max_input_tokens']:
            raise ValueError('JEV question expansion exceeds max_input_tokens; no truncation')
        shape = (plan[0][4], plan[0][3]) if len(plan) == 1 else None
        return self._submit(request, plan, max(item[4] for item in plan), shape, workspace_extra=total * 24)

    def _execute(self, index, request, plan):
        lane, answers = self.lanes[index], {}
        with self.torch.inference_mode(), self.torch.cuda.stream(lane['stream']):
            for key, question, prompt, count, tokens, token_ids in plan:
                scores = self._score(index, prompt, token_ids, count)
                answers[key] = self.protocol.formatter.to_answer(question, self.protocol.calibrate(scores))
        return {'answers': answers, 'usage': {'input_tokens': sum(item[4] for item in plan), 'output_tokens': len(plan)}}

    def _native(self, prompt, token_ids, count, snapshot=False):
        self.stats['native'] += 1
        allowed = self.protocol.readout['readout_token_ids'][:count]
        return self.native_executor.submit(self.runtime.native, prompt, token_ids, allowed, snapshot=snapshot).result()

    def _score(self, index, prompt, token_ids, count):
        torch, lane = self.torch, self.lanes[index]
        tokens, shape = len(token_ids), (len(token_ids), count)
        key = (index, shape)
        hot = self.cache.observe(key)
        with self.gate.enter():
            self._healthy()
            entry = self.cache.get(key)
            if entry is not None and not self.cache.disabled:
                entry.uses += 1
                if entry.uses % self.config['verify_every']:
                    try:
                        scores = self._replay(entry, token_ids)
                        self.stats['hits'] += 1
                        return scores
                    finally: lane['stream'].synchronize()
        # The single native runner owns global Python forward/scheduler state.
        # It never overlaps either a capture or a graph replay.
        with self.gate.enter(exclusive=True):
            self._healthy()
            entry = self.cache.get(key)
            reserve = self._estimate(tokens) + 128 * MiB
            capture = (entry is None and hot and tokens <= self.config['max_graph_tokens']
                       and self.cache.admit(reserve, key))
            expected, snapshot = self._native(prompt, token_ids, count, snapshot=entry is not None or capture)
            if self.cache.disabled: return expected
            if capture: entry = self._capture(index, shape, snapshot, expected, reserve)
            if entry is None: return expected
            actual = self._replay(entry, token_ids)
            self.stats['checks'] += 1
            equal = actual == expected and torch.equal(entry.hidden[:tokens], snapshot[2])
            lane['stream'].synchronize()
            if not equal:
                self._disable_graphs()
                return expected
            self.stats['hits'] += 1
            return actual

    def _disable_graphs(self):
        self.stats['mismatches'] += 1
        self.cache.disabled = True
        self.cache.clear()
        self.parallel = 1
        self.torch.cuda.empty_cache()

    def _replay(self, entry, token_ids):
        torch = self.torch
        entry.inputs['input_ids'][:len(token_ids)].copy_(torch.tensor(token_ids, dtype=entry.inputs['input_ids'].dtype))
        entry.graph.replay()
        # Copy while the lane and GPU gate still own the graph's output.
        return entry.scores.cpu().tolist()[0]

    def _capture(self, index, shape, snapshot, expected, reserve):
        from vllm.forward_context import override_forward_context
        from vllm.v1.worker.gpu.sample.logprob import compute_token_logprobs
        torch, lane = self.torch, self.lanes[index]
        tokens, count = shape
        self.cache.make_room(reserve)
        torch.cuda.empty_cache()
        self.cache.captures += 1
        started = time.monotonic()
        entry = JEVEntry()
        try:
            clone = TensorClone(torch)
            entry.inputs = clone(snapshot[0])
            entry.context = lane['state'].context(snapshot[1], torch, tokens, clone)
            entry.allowed = torch.tensor(self.protocol.readout['readout_token_ids'][:count], device='cuda')
            entry.row = torch.tensor([tokens - 1], device='cuda')
            # Match the native top-k logprob kernel's count+1 gather width.
            entry.read_ids = torch.cat((entry.allowed[:1], entry.allowed)).view(1, -1)
            def forward():
                hidden = lane['model'](**entry.inputs)
                logits = lane['model'].compute_logits(hidden.index_select(0, entry.row)).float()
                allowed = logits.index_select(1, entry.allowed)
                logits.fill_(-float('inf'))
                logits.index_copy_(1, entry.allowed, allowed)
                scores = compute_token_logprobs(logits, entry.read_ids)[:, 1:]
                return hidden, scores
            with override_forward_context(entry.context):
                hidden, scores = forward()
            lane['stream'].synchronize()
            if scores.cpu().tolist()[0] != expected or not torch.equal(hidden[:tokens], snapshot[2]):
                self._disable_graphs()
                return None
            del hidden, scores
            release_lane_workspace(torch, lane['stream'])
            torch.cuda.empty_cache()
            cap = min(self.memory_limit, torch.cuda.memory_reserved() + reserve)
            torch.cuda.set_per_process_memory_fraction(cap / self.total_memory, 0)
            entry.graph = torch.cuda.CUDAGraph()
            with torch.cuda.graph(entry.graph, stream=lane['stream']), override_forward_context(entry.context):
                entry.hidden, entry.scores = forward()
            lane['stream'].synchronize()
            release_lane_workspace(torch, lane['stream'])
            entry.bytes = graph_pool_bytes(torch, entry.graph) + clone.bytes + (2 * count + 2) * 8
            entry.uses = 0
            if entry.bytes > reserve:
                entry.close()
                self.stats['capture_rejected'] += 1
                return None
            self.cache.put((index, shape), entry)
            return entry
        except BaseException as error:
            self.failed = 'JEV graph capture failed: ' + str(error)[:512]
            with self.condition: self.condition.notify_all()
            raise
        finally:
            torch.cuda.set_per_process_memory_fraction(self.memory_limit / self.total_memory, 0)
            self.cache.capture_seconds += time.monotonic() - started
            self.stats['reserved_peak_bytes'] = max(self.stats['reserved_peak_bytes'], torch.cuda.memory_reserved())

    def execution_status(self):
        result = super().execution_status()
        result['lane_state_bytes'] = sum(lane['state'].bytes for lane in self.lanes)
        result['native_parallel'] = 1
        result['scratch_fix'] = 'flashinfer-sm90-private-workspace-v1'
        return result

    def close(self):
        try: super().close()
        finally:
            try: self.native_executor.submit(self.runtime.close).result()
            finally: self.native_executor.shutdown(wait=True)
