"""Nullable objects retain their strict nested response contract."""
import pytest
from demiflow.schema import compile_schema,validate_instance,inspect_schema,SchemaError,SchemaValidationError


def test_nullable_object_keeps_required_fields_and_scalar_types():
    schema=compile_schema({'type':'object','properties':{'task':{'type':['object','null'],
        'properties':{'direction':{'type':'string','minLength':1}},'required':['direction'],
        'additionalProperties':False}},'required':['task'],'additionalProperties':False})
    validate_instance({'task':None},schema)
    validate_instance({'task':{'direction':'a meaningful direction'}},schema)
    for value in [{},{'task':{}},{'task':[]},{'task':{'direction':7}},{'task':{'direction':''}},
                  {'task':{'direction':'ok','extra':1}}]:
        with pytest.raises(SchemaValidationError):validate_instance(value,schema)
    assert schema['properties']['task']['type']==['object','null']


@pytest.mark.parametrize('kind',[['object','string'],['null','null'],['null'],{},['object','null','string']])
def test_unsupported_union_reports_schema_error_not_unhashable(kind):
    value={'type':'object','properties':{'task':{'type':kind}}}
    assert inspect_schema(value)[1]
    with pytest.raises(SchemaError):compile_schema(value)


def test_nullable_nested_schema_is_still_inspected():
    value={'type':'object','properties':{'task':{'type':['object','null'],
        'properties':{'bad':{'type':'array','items':{'type':'wrong'}}}}}}
    assert inspect_schema(value)[1]
    with pytest.raises(SchemaError):compile_schema(value)
