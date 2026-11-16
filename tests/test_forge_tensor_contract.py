"""Static tensor contracts never execute or import candidate PyTorch code."""
import pytest
from demiflow.forge.source import SourceTree, InspectionError
from demiflow.forge.operator_check import OperatorChecker


def inspect(tmp_path, body, *, resources=None, returns='float', helpers=''):
    (tmp_path/'ops.py').write_text('import torch\n'+helpers+'\ndef convert(value: int) -> '+returns+':\n'+body)
    tree=SourceTree(tmp_path);checker=OperatorChecker(tree)
    checker.check(checker.describe(tree.load('ops')['convert']),
                  native_resources=resources if resources is not None else {'gpu':1,'memory_bytes':1024**2})
    return checker


@pytest.mark.parametrize('expression,returns', [
    ("(torch.arange(8,dtype=torch.float32,device='cuda') * 2).sum().item()",'float'),
    ("torch.tensor([[1,2],[3,4]],dtype=torch.int64,device='cuda').tolist()",'list[list[int]]'),
    ("torch.ones((2,3),dtype=torch.float64,device='cuda').mean().item()",'float'),
    ("torch.full((1,),3,dtype=torch.int32,device='cuda').item()",'int'),
    ("torch.zeros((2,),dtype=torch.bool,device='cpu').tolist()",'list[bool]'),
    ('torch.cuda.device_count()','int'), ('torch.cuda.is_available()','bool'),
])
def test_known_finite_tensor_protocol(tmp_path,expression,returns):
    checked=inspect(tmp_path,'    return '+expression+'\n',returns=returns)
    assert checked.records and checked.records[-1]['behavior']=='not_run'


@pytest.mark.parametrize('body,resources,message', [
    ("return torch.arange(2,dtype=torch.float32,device='cuda').sum().item()",{'memory_bytes':1024},'GPU resource'),
    ("return torch.arange(2,dtype=torch.float32,device='cpu').sum().item()",{},'memory_bytes'),
    ("return torch.arange(value,dtype=torch.float32,device='cuda').sum().item()",None,'static literals'),
    ("return torch.arange(16,dtype=torch.float32,device='cuda').sum().item()",{'gpu':1,'memory_bytes':32},'finite per-call'),
    ("return torch.rand(2)",None,'Unknown PyTorch'),
    ("return torch.load('file.pt')",None,'Unknown PyTorch'),
    ("return torch.cuda.set_device(0)",None,'Unknown PyTorch'),
    ("return torch.arange(2,dtype=torch.float32,device='cuda').to('cpu')",None,'dynamic options'),
    ("return torch.arange(2,dtype=torch.float32,device='cuda').item()",None,'single tensor element'),
    ("return torch.arange(2,dtype=torch.float32,device='cuda:1').sum().item()",None,'admitted CUDA device'),
    ("return torch.arange(2).sum().item()",None,'explicit dtype and device'),
    ("return torch.tensor([[1],[2,3]],dtype=torch.int64,device='cuda').sum().item()",None,'Ragged'),
    ("return torch.arange(1048577,dtype=torch.float32,device='cuda').sum().item()",None,'finite shape'),
    ("return torch.arange(2,dtype=torch.float32,device='cuda') ** 2",None,'arithmetic protocol'),
    ("for i in range(2):\n        t = torch.ones((2,),dtype=torch.float32,device='cuda')\n    return 1.0",None,'loops/comprehensions'),
])
def test_unknown_effect_shape_device_and_budget_fail_closed(tmp_path,body,resources,message):
    with pytest.raises(InspectionError,match=message):
        inspect(tmp_path,'    '+body+'\n',resources=resources)


def test_helper_inherits_exact_resource_declaration_and_transient_budget(tmp_path):
    helper="def tensor() -> float:\n    return torch.ones((8,),dtype=torch.float32,device='cuda').sum().item()\n"
    with pytest.raises(InspectionError,match='finite per-call'):
        inspect(tmp_path,'    return tensor() + tensor()\n',resources={'gpu':1,'memory_bytes':64},helpers=helper)


def test_torch_import_does_not_admit_external_effects_or_shadowing(tmp_path):
    with pytest.raises(InspectionError,match='Undeclared external effect'):
        inspect(tmp_path,"    return open('secret.txt')\n")
    (tmp_path/'torch.py').write_text('def arange():\n    pass\n')
    with pytest.raises(InspectionError,match='shadows trusted namespace'):
        inspect(tmp_path,'    return torch.cuda.device_count()\n',returns='int')


def test_nested_singleton_list_headers_share_the_original_call_budget(tmp_path):
    body="    return torch.ones((1,1,1,1),dtype=torch.float32,device='cuda').tolist()\n"
    with pytest.raises(InspectionError,match='finite per-call'):
        inspect(tmp_path,body,resources={'gpu':1,'memory_bytes':128},returns='list[list[list[list[float]]]]')
    assert inspect(tmp_path,body,resources={'gpu':1,'memory_bytes':512},returns='list[list[list[list[float]]]]').records
