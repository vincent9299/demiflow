"""Graph native tensor stages without reimplementing model protocols or numerics."""
import copy
import threading
import time
from types import SimpleNamespace

try:
    from .cuda_graphs import BoundedExecution, RequestLease, check_encoding_budget, qwen_workspace_bytes, graph_pool_bytes
except ImportError:
    from cuda_graphs import BoundedExecution, RequestLease, check_encoding_budget, qwen_workspace_bytes, graph_pool_bytes


def shared_weights_clone(model):
    clone = copy.deepcopy(model, {id(p): p for p in model.parameters()})
    if not all(a.data_ptr() == b.data_ptr() for a, b in zip(model.parameters(), clone.parameters(), strict=True)):
        raise RuntimeError('Model clone did not share weights')
    return clone


def tree_map(value, fn):
    if isinstance(value, tuple): return tuple(tree_map(x, fn) for x in value)
    if isinstance(value, list): return [tree_map(x, fn) for x in value]
    if isinstance(value, dict): return {k: tree_map(v, fn) for k, v in value.items()}
    return fn(value)


def tree_pairs(a, b):
    if isinstance(a, (tuple, list)):
        for x, y in zip(a, b, strict=True): yield from tree_pairs(x, y)
    elif isinstance(a, dict):
        if a.keys() != b.keys(): raise RuntimeError('Native tensor tree keys changed')
        for k in a: yield from tree_pairs(a[k], b[k])
    else: yield a, b


def signature(value, torch):
    if isinstance(value, torch.Tensor):
        return ('tensor', tuple(value.shape), tuple(value.stride()), str(value.dtype), str(value.device))
    if isinstance(value, (list, tuple)):
        return (type(value).__name__, tuple(signature(x, torch) for x in value))
    if isinstance(value, dict):
        return ('dict', tuple((k, signature(v, torch)) for k, v in value.items()))
    if value is None or type(value) in (int, float, bool, str): return (type(value).__name__, value)
    raise ValueError('Unsupported native CUDA graph argument: '+type(value).__name__)


class TensorEntry:
    def close(self):
        self.graph.reset()
        self.graph = self.arguments = self.output = None


class NativeGraphAdapter(BoundedExecution):
    def __init__(self, backend, configuration):
        self.backend = backend
        self._initialize(configuration, backend.workspace_estimate, backend.profile+'_cuda_graphs')
        for index in range(self.parallel):
            self._add_lane(backend.clone(self, index), lease=None, graph_allowed=False)
        self._finish_init()

    def single(self, request):
        questions = request.get('questions') if isinstance(request, dict) else None
        if not isinstance(questions, dict) or not 1 <= len(questions) <= 128:
            raise ValueError('Expected 1..128 questions')
        check_encoding_budget(request, self.config['max_input_tokens'])
        self._healthy()
        with self.encoding:
            plan, tokens = self.backend.prepare(request)
        return self._submit(request, plan, tokens, workspace_extra=self.backend.admission_extra(plan, tokens))

    def _execute(self, index, request, plan):
        lane = self.lanes[index]
        lane['graph_allowed'] = not bool(request.get('images') or request.get('videos'))
        with self.torch.inference_mode(), self.torch.cuda.stream(lane['stream']):
            if not lane['graph_allowed']:
                with self.gate.enter(exclusive=True):
                    self._healthy()
                    try: return self.backend.execute(lane['model'], request, plan)
                    finally: lane['stream'].synchronize()
            with RequestLease(self.gate, lane['stream'].synchronize) as lease:
                lane['lease'] = lease
                try:
                    self._healthy()
                    return self.backend.execute(lane['model'], request, plan)
                finally: lane['lease'] = None

    def call(self, index, stage, native, args, kwargs, tokens, retained_bytes=0, capture_native=None):
        torch, lane = self.torch, self.lanes[index]
        def eager():
            self.stats['native'] += 1
            return native(*args, **kwargs)
        if not lane['graph_allowed'] or self.cache.disabled or tokens > self.config['max_graph_tokens']:
            return eager()
        arguments = (args, kwargs)
        shape = (stage, signature(arguments, torch))
        key = (index, shape)
        hot = self.cache.observe(key)
        entry = self.cache.get(key)
        if entry is not None:
            entry.uses += 1
            if entry.uses % self.config['verify_every']:
                self.stats['hits'] += 1
                return self._replay(entry, arguments)
            # A periodic audit does not mutate the graph or its private pool.
            # Native execution and replay remain on this request's own stream.
            expected = eager()
            actual = self._replay(entry, arguments)
            self.stats['checks'] += 1
            equal = all(torch.equal(a, b) if isinstance(a, torch.Tensor) else a == b
                        for a, b in tree_pairs(actual, expected))
            lane['stream'].synchronize()
            if equal:
                self.stats['hits'] += 1
                return actual
            self.stats['mismatches'] += 1
            self.cache.disabled = True
            # Readers already using an entry must finish before any pool frees.
            with lane['lease'].exclusive():
                self.cache.clear()
                torch.cuda.empty_cache()
            return expected
        # Include input cache copies and outputs which outlive the hook.
        input_bytes = sum(a.nbytes for a, _ in tree_pairs(arguments, arguments) if isinstance(a, torch.Tensor))
        reserve = self._estimate(tokens) + 3 * input_bytes + retained_bytes
        if entry is None and (not hot or not self.cache.admit(reserve, key)):
            return eager()
        # RequestLease still owns a reader here; the independent reference can
        # overlap other lanes. exclusive() synchronizes it before pool changes.
        expected = eager()
        with lane['lease'].exclusive():
            self._healthy()
            if self.cache.disabled: return expected
            entry = self.cache.get(key)
            if entry is None and self.cache.admit(reserve, key):
                entry = self._capture(index, shape, capture_native or native, arguments, reserve)
            if entry is None: return expected
            actual = self._replay(entry, arguments)
            self.stats['checks'] += 1
            equal = all(torch.equal(a, b) if isinstance(a, torch.Tensor) else a == b
                        for a, b in tree_pairs(actual, expected))
            lane['stream'].synchronize()
            if not equal:
                self.stats['mismatches'] += 1
                self.cache.disabled = True
                self.cache.clear()
                torch.cuda.empty_cache()
                return expected
            self.stats['hits'] += 1
            return actual

    def _replay(self, entry, arguments):
        torch = self.torch
        for dest, source in tree_pairs(entry.arguments, arguments):
            if isinstance(dest, torch.Tensor): dest.copy_(source)
        entry.graph.replay()
        # Native postprocessing may outlive the graph cache entry. Never expose
        # private-pool output addresses to another stage or a suspended request.
        return tree_map(entry.output, lambda x: x.clone() if isinstance(x, torch.Tensor) else x)

    def _capture(self, index, shape, native, arguments, reserve):
        torch, lane = self.torch, self.lanes[index]
        self.cache.make_room(reserve)
        torch.cuda.empty_cache()
        self.cache.captures += 1
        start = time.monotonic()
        entry = TensorEntry()
        try:
            entry.arguments = tree_map(arguments, lambda x: x.clone() if isinstance(x, torch.Tensor) else x)
            def forward():
                if __package__:
                    from .capture_kernels import capture_kernels
                else:
                    from capture_kernels import capture_kernels
                with capture_kernels(self.backend.capture_model(lane['model']), self.gate):
                    return native(*entry.arguments[0], **entry.arguments[1])
            # Static clones can have a different layout from native views;
            # warm that layout once before capture, preserving lazy setup.
            forward()
            lane['stream'].synchronize()
            torch._C._cuda_clearCublasWorkspaces()
            torch.cuda.empty_cache()
            cap = min(self.memory_limit, torch.cuda.memory_reserved() + reserve)
            torch.cuda.set_per_process_memory_fraction(cap / self.total_memory, 0)
            entry.graph = torch.cuda.CUDAGraph()
            with torch.cuda.graph(entry.graph, stream=lane['stream']): entry.output = forward()
            lane['stream'].synchronize()
            torch._C._cuda_clearCublasWorkspaces()
            pool_bytes = graph_pool_bytes(torch, entry.graph)
            static_bytes = sum(a.nbytes for a, _ in tree_pairs(entry.arguments, entry.arguments) if isinstance(a, torch.Tensor))
            entry.bytes = pool_bytes + static_bytes
            entry.uses = 0
            if entry.bytes > reserve:
                entry.close()
                self.stats['capture_rejected'] += 1
                return None
            self.cache.put((index, shape), entry)
            return entry
        except BaseException as error:
            self.failed = 'CUDA graph capture failed: ' + str(error)[:512]
            with self.condition: self.condition.notify_all()
            raise
        finally:
            torch.cuda.set_per_process_memory_fraction(self.memory_limit / self.total_memory, 0)
            self.cache.capture_seconds += time.monotonic() - start
            self.stats['reserved_peak_bytes'] = max(self.stats['reserved_peak_bytes'], torch.cuda.memory_reserved())


class AmosBackend:
    profile = 'amos'
    def capture_model(self, engine): return engine.model
    def workspace_estimate(self, tokens):
        return qwen_workspace_bytes(self.backbone.config.text_config, tokens)
    def admission_extra(self, plan, tokens): return 0
    def __init__(self, engine):
        self.engine, self.backbone = engine, engine.model
        if (engine.rotations != 1 or engine.think_below or engine.think_url
                or not getattr(engine.model.config, 'amos_noncausal', False)
                or engine.model.config.text_config.use_cache):
            raise ValueError('AMOS graphs require native noncausal single-pass execution without KV cache')

    def prepare(self, request):
        try:
            from .model_adapters import validate_request
        except ImportError:
            from model_adapters import validate_request
        from amos.serve import parse
        from amos.decoder import expand
        request = validate_request(request)
        state, _, questions = parse(request)
        flat, _ = expand(questions)
        images = [self.engine.image(x) for x in request.get('images', [])]
        encoded = self.engine.codec.encode_case(state, flat, images)
        if len(encoded[0]) > self.engine.MAX_LEN:
            raise ValueError('Request exceeds native AMOS maximum context; input was not truncated')
        return (request, state, flat, encoded), len(encoded[0])

    def clone(self, runtime, index):
        engine = copy.copy(self.engine)
        engine.model = shared_weights_clone(self.engine.model)
        engine.lock = threading.Lock()
        # Heads retain references to model modules. Keep their native execution;
        # noul/choice do not invoke trained span/region heads.
        decoder = engine.model.get_decoder()
        native = decoder.forward
        def hidden(*args, **kwargs): return native(*args, **kwargs).last_hidden_state
        def forward(*args, **kwargs):
            # Image calls pass through another native module and are not wrapped.
            ids = kwargs.get('input_ids')
            if args or ids is None or set(kwargs)-{'input_ids','attention_mask'}:
                return native(*args, **kwargs)
            value = runtime.call(index, 'decoder', hidden, args, kwargs, ids.numel())
            return SimpleNamespace(last_hidden_state=value)
        decoder.forward = forward
        return engine

    def execute(self, engine, request, plan):
        request, state, flat, encoded = plan
        codec = self.engine.codec
        class PreparedCodec:
            def __getattr__(self, key): return getattr(codec, key)
            def encode_case(self, s, qs, images=()):
                if s != state or qs != flat:
                    raise RuntimeError('Native AMOS encoding contract changed')
                return encoded
        engine.codec = PreparedCodec()
        try: return engine(request)
        finally: engine.codec = codec


class D3Backend:
    profile = 'd3'
    def capture_model(self, decider): return decider.model.backbone
    def workspace_estimate(self, tokens):
        return qwen_workspace_bytes(self.backbone.config.text_config, tokens)
    def workspace_extra(self, tokens):
        # Native convolution tails are views of the entire projection. Count
        # retained storage, plus fixed recurrent matrices, not just view.nbytes.
        c = self.backbone.config.text_config
        linear = c.layer_types.count('linear_attention')
        full = c.layer_types.count('full_attention')
        recurrent = c.linear_num_value_heads * c.linear_key_head_dim * c.linear_value_head_dim * 4
        projection = tokens * (2*c.linear_num_key_heads*c.linear_key_head_dim
                               + c.linear_num_value_heads*c.linear_value_head_dim) * 2
        kv = tokens * 2*c.num_key_value_heads*c.head_dim*2
        return 2 * (linear*(recurrent+projection) + full*kv)

    def __init__(self, decider):
        self.decider, self.backbone = decider, decider.model.backbone
        model = decider.model
        if model.state_once is None or model.fast is not None:
            raise ValueError('D3 graphs require the native state-once execution path')
        # All lane contexts then enter/restore identical SDPA flags. No thread
        # can re-enable cuDNN while another is still computing native attention.
        import torch
        torch.backends.cuda.enable_cudnn_sdp(False)

    def prepare(self, request):
        from d3_server import Refusal
        try: prepared = self.decider.model.prepare(**self.decider.arguments(request))
        except Refusal as error: raise ValueError(str(error.detail)) from error
        if prepared.errors:
            # Let native answer format/refuse invalid questions without GPU work.
            return prepared, 1
        lengths = (list(prepared.lengths.values()) + list(prepared.reversed_lengths.values())
                   if prepared.images or prepared.videos else
                   [len(x) for x in [*prepared.sequences.values(), *prepared.reversed_sequences.values()]])
        state = max(prepared.states.values(), default=0)
        # Native state-once reads the state once, then each question block.
        tokens = state + sum(length-state for length in lengths)
        return prepared, max(1, tokens)

    def admission_extra(self, plan, tokens):
        return self.workspace_extra(max(plan.states.values(), default=0))

    def clone(self, runtime, index):
        from d3_runtime import StateOnce, PromptEncoder
        decider = copy.copy(self.decider)
        decider.turn = threading.Lock()
        model = decider.model = copy.copy(self.decider.model)
        model.backbone = shared_weights_clone(self.backbone)
        model.encoder = PromptEncoder(model.backbone.language_model)
        model.state_once = StateOnce(model.backbone.language_model)
        native_state, native_questions = model.state_once.state, model.state_once.questions
        model.state_once.state = lambda embeds, rope: runtime.call(
            index, 'state', native_state, (embeds, rope), {}, embeds.shape[1],
            retained_bytes=self.workspace_extra(embeds.shape[1]))
        state_once = model.state_once
        def reference(cache, embeds, lengths, rope, present, last):
            return native_questions(cache, embeds, lengths, rope)
        def tensor_questions(cache, embeds, lengths, rope, present, last):
            # Only hoist the native CPU-list -> CUDA index construction. Keep
            # native layers, precision, padding, recurrent state and final norm.
            hidden = embeds * present[..., None].to(embeds.dtype)
            for (layer, softmax), entry in zip(state_once.stack, cache):
                hidden, _ = state_once._layer(layer, softmax, hidden, rope, entry, present)
            return state_once.language_model.norm(
                hidden[runtime.torch.arange(len(lengths), device=hidden.device), last])
        def questions(cache, embeds, lengths, rope):
            torch = runtime.torch
            present = torch.arange(embeds.shape[1], device=embeds.device)[None] < torch.as_tensor(
                list(lengths), device=embeds.device)[:, None]
            last = torch.as_tensor([n-1 for n in lengths], device=embeds.device)
            return runtime.call(index, 'questions', reference,
                (cache, embeds, lengths, rope, present, last), {}, embeds.shape[0]*embeds.shape[1],
                capture_native=tensor_questions)
        model.state_once.questions = questions
        return decider

    def execute(self, decider, request, plan):
        original = decider.model.prepare
        def prepared(state, questions, images=None, videos=None):
            if state != request.get('state') or questions != request['questions']:
                raise RuntimeError('Native D3 preparation contract changed')
            return plan
        decider.model.prepare = prepared
        try: return decider.answer(request)
        finally: decider.model.prepare = original
