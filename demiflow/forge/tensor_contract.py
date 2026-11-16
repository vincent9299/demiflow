"""Closed static PyTorch value protocol; never imports or executes PyTorch.

Only explicit finite shapes/dtypes/devices and value operations are admitted.
This is neither an opaque callable permission nor a model/IO/device-management
API. Tensor ports remain internal: business boundaries return checked scalars
or lists. Allocations are conservatively summed, including transient results;
loops/comprehensions and unknown shapes are refused rather than estimated.
"""
import ast
import math

DTYPES = {'torch.float32': ('float', 4), 'torch.float64': ('float', 8),
          'torch.int32': ('int', 4), 'torch.int64': ('int', 8), 'torch.bool': ('bool', 1)}
MAX_ELEMENTS = 1024**2
MAX_BYTES = 64*1024**2


def _fail(checker, message, node, module):
    checker.fail('OP-06', message, node, module, incomplete=True)


def _charge(checker, size, node, module):
    resources, loop, concurrency = checker.tensor_contexts[-1]
    if loop:
        _fail(checker, 'Tensor computation inside loops/comprehensions is not supported', node, module)
    memory = resources.get('memory_bytes')
    if type(memory) is not int or memory <= 0:
        _fail(checker, 'Tensor computation requires an explicit native memory_bytes declaration', node, module)
    checker.tensor_allocated += size
    if checker.tensor_allocated > min(MAX_BYTES, memory // concurrency):
        _fail(checker, 'Tensor allocation and temporary results exceed the declared finite per-call window', node, module)


def _tensor(checker, shape, dtype, device, node, module):
    from .operator_check import Value
    if len(shape) > 4 or any(type(n) is not int or not 0 < n <= MAX_ELEMENTS for n in shape):
        _fail(checker, 'Tensor shape must be a finite positive literal of rank at most 4', node, module)
    elements = math.prod(shape)
    if elements > MAX_ELEMENTS:
        _fail(checker, 'Tensor element count exceeds the finite inspection window', node, module)
    _charge(checker, elements*DTYPES[dtype][1], node, module)
    return Value(dict(kind='tensor', nullable=False, shape=list(shape), dtype=dtype, device=device),
                 owns_container=True)


def _literal(checker, node, module):
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, RecursionError):
        _fail(checker, 'Tensor shapes and constructor data must be static literals', node, module)


def construct(checker, name, args, kwargs, node, module):
    from .operator_check import Value
    from .source import Symbol
    from ..data.flow_contract import scalar
    if not name.startswith('torch.'):
        return None
    if checker.tree.is_local(name):
        _fail(checker, 'Tensor protocol cannot bind a workspace shadow of PyTorch', node, module)
    if name in {'torch.cuda.device_count', 'torch.cuda.is_available'} and not args and not kwargs:
        # Read-only observations of the original worker's visibility. These
        # neither select a device nor authorize allocation or new device pools.
        return Value(scalar('int' if name.endswith('device_count') else 'bool'))
    if name not in {'torch.arange', 'torch.tensor', 'torch.zeros', 'torch.ones', 'torch.full'}:
        _fail(checker, 'Unknown PyTorch API has no managed tensor contract: '+name, node, module)
    if set(kwargs) != {'dtype', 'device'}:
        _fail(checker, 'Tensor construction requires explicit dtype and device only', node, module)
    dtype, device = kwargs['dtype'], kwargs['device']
    if not isinstance(dtype, Symbol) or dtype.name not in DTYPES:
        _fail(checker, 'Tensor dtype must be an explicit supported PyTorch dtype', node, module)
    if not isinstance(device, Value) or device.literal not in {'cpu','cuda','cuda:0'}:
        _fail(checker, 'Tensor device must be explicit cpu or the admitted CUDA device', node, module)
    device = device.literal
    resources = checker.tensor_contexts[-1][0]
    if device != 'cpu' and resources.get('gpu', 0) < 1:
        _fail(checker, 'CUDA tensor requires this operator native GPU resource declaration', node, module)
    if name == 'torch.arange':
        values = [_literal(checker,n,module) for n in node.args]
        if not 1 <= len(values) <= 3 or any(type(v) is not int or abs(v)>2**63-1 for v in values):
            _fail(checker, 'arange requires one to three integer literals', node, module)
        start, end, step = (0,values[0],1) if len(values)==1 else (*values,1) if len(values)==2 else values
        if step == 0 or abs(end-start) > MAX_ELEMENTS*abs(step):
            _fail(checker, 'arange exceeds the finite shape window', node, module)
        shape = [len(range(start,end,step))]
    elif name == 'torch.tensor':
        if len(node.args) != 1:
            _fail(checker, 'tensor requires one finite literal value', node, module)
        value = _literal(checker,node.args[0],module)
        def shape_of(value, depth=0):
            if depth > 4:
                _fail(checker,'Tensor literal rank exceeds 4',node,module)
            if (type(value) in (int,bool) and abs(value)<=2**63-1
                    or type(value) is float and math.isfinite(value)):
                return []
            if type(value) not in (list,tuple) or not value or len(value)>MAX_ELEMENTS:
                _fail(checker,'Tensor literal must contain finite numeric values',node,module)
            shapes=[shape_of(v,depth+1) for v in value]
            if any(s!=shapes[0] for s in shapes):
                _fail(checker,'Ragged tensor literal is not supported',node,module)
            return [len(value),*shapes[0]]
        shape=shape_of(value)
    else:
        if len(node.args) != (2 if name=='torch.full' else 1):
            _fail(checker,'Tensor constructor has unsupported positional arguments',node,module)
        shape=_literal(checker,node.args[0],module)
        if type(shape) is int: shape=[shape]
        if type(shape) not in (list,tuple):
            _fail(checker,'Tensor shape must be a literal integer sequence',node,module)
        if name=='torch.full':
            fill=_literal(checker,node.args[1],module)
            if not (type(fill) in (int,bool) and abs(fill)<=2**63-1
                    or type(fill) is float and math.isfinite(fill)):
                _fail(checker,'Tensor fill must be a finite numeric literal',node,module)
    return _tensor(checker,shape,dtype.name,device,node,module)


def binary(checker, values, node, module):
    from .operator_check import Value
    tensors=[v for v in values if isinstance(v,Value) and v.type['kind']=='tensor']
    if not tensors:
        return None
    if not isinstance(node,ast.BinOp) or not isinstance(node.op,(ast.Add,ast.Sub,ast.Mult,ast.Div)):
        _fail(checker,'Unsupported tensor arithmetic protocol',node,module)
    exemplar=tensors[0].type
    if exemplar['dtype']=='torch.bool':
        _fail(checker,'Boolean tensor arithmetic is not supported',node,module)
    if any(v.type != exemplar for v in tensors):
        _fail(checker,'Tensor operands require the same shape/dtype/device',node,module)
    for v in values:
        if not isinstance(v,Value) or v.type['kind'] not in {'tensor','int','float','bool'} or v.type.get('nullable'):
            _fail(checker,'Tensor arithmetic requires tensor or non-null numeric scalar operands',node,module)
        if exemplar['dtype'] in {'torch.int32','torch.int64','torch.bool'} and v.type['kind']=='float':
            _fail(checker,'Implicit tensor dtype promotion is not supported',node,module)
    if isinstance(node.op,ast.Div) and DTYPES[exemplar['dtype']][0]!='float':
        _fail(checker,'Tensor division requires explicit floating dtype',node,module)
    return _tensor(checker,exemplar['shape'],exemplar['dtype'],exemplar['device'],node,module)


def method(checker, parent, name, args, kwargs, node, module):
    from .operator_check import Value
    from ..data.flow_contract import scalar
    if parent.type['kind'] != 'tensor':
        return None
    if args or kwargs:
        _fail(checker,'Tensor value method does not accept dynamic options',node,module)
    kind=DTYPES[parent.type['dtype']][0]
    if name in {'sum','mean'}:
        if kind not in {'float','int'} or name=='mean' and kind!='float':
            _fail(checker,'Tensor reduction requires a supported numeric dtype',node,module)
        dtype = 'torch.int64' if kind=='int' else parent.type['dtype']
        return _tensor(checker,[],dtype,parent.type['device'],node,module)
    if name=='item':
        if math.prod(parent.type['shape']) != 1:
            _fail(checker,'item requires a single tensor element',node,module)
        return Value(scalar(kind))
    if name=='tolist':
        shape = parent.type['shape']
        # Include every nested list header and pointer array, not just leaves.
        # Singleton dimensions can retain four containers for one scalar.
        containers = sum(math.prod(shape[:depth]) for depth in range(len(shape)))
        slots = sum(math.prod(shape[:depth+1]) for depth in range(len(shape)))
        _charge(checker,64*math.prod(shape)+64*containers+8*slots,node,module)
        result=scalar(kind)
        for dimension in reversed(parent.type['shape']):
            result={**scalar('list'),'item':result}
        return Value(result,owns_container=True)
    _fail(checker,'Unknown tensor method has no managed contract: '+name,node,module)
