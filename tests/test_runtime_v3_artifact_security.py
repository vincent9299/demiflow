"""Guards for V033's executed evidence, plus the real formal node elsewhere."""
from copy import deepcopy
from types import SimpleNamespace
import pytest

from runtime_v3_artifact_security_scenarios import REQUIRED_CASES, validate_evidence


def _guard_sample():
    """Synthetic evidence only tests the guard; it never counts as V033 proof."""
    cases = {name:{'status':'rejected', 'exception':'DataflowError', 'code':'INVALID_CONTRACT',
                   'message':'guard fixture', 'original_sha256':'a'*64, 'restored_sha256':'a'*64,
                   'ref':{'digest':'a'*64}} for name in REQUIRED_CASES}
    cases['oversized_bootstrap'] = {'status':'rejected','returncode':1,
        'stderr':'Worker launch spec exceeds byte budget'}
    return {'counterexamples':cases, 'positive':{'output':[
        {'value':2,'payload_bytes':98304},{'value':3,'payload_bytes':98304}],
        'commits':[{'status':'acked'}]}, 'launches':[{'bytes':1500,'reference_only':True}],
        'control_frames':{'WorkerEvent:cut':{'count':1,'maximum_bytes':200,'bound':65536,
                                         'business_payload_matches':0}},
        'current_main_reads':[], 'historical_recompiles':0,'original_objects_restored':True}


@pytest.mark.parametrize('defect', ['empty','missing_case','accepted','missing_exception',
    'bootstrap_not_rejected','changed_original','wrong_output','missing_ack','not_acked',
    'missing_launch','big_launch','inline_assignment','missing_control','control_payload',
    'control_bound','current_main','recompile','not_restored'])
def test_security_guard_rejects_incomplete_or_unbound_proof(defect):
    evidence = deepcopy(_guard_sample())
    if defect == 'empty': evidence = {}
    elif defect == 'missing_case': evidence['counterexamples'].pop('expired_grant')
    elif defect == 'accepted': evidence['counterexamples']['checksum']['status'] = 'accepted'
    elif defect == 'missing_exception': evidence['counterexamples']['checksum']['exception'] = None
    elif defect == 'bootstrap_not_rejected': evidence['counterexamples']['oversized_bootstrap']['returncode'] = 0
    elif defect == 'changed_original': evidence['counterexamples']['checksum']['restored_sha256'] = 'b'*64
    elif defect == 'wrong_output': evidence['positive']['output'].pop()
    elif defect == 'missing_ack': evidence['positive']['commits'] = []
    elif defect == 'not_acked': evidence['positive']['commits'][0]['status'] = 'prepared'
    elif defect == 'missing_launch': evidence['launches'] = []
    elif defect == 'big_launch': evidence['launches'][0]['bytes'] = 65537
    elif defect == 'inline_assignment': evidence['launches'][0]['reference_only'] = False
    elif defect == 'missing_control': evidence['control_frames'] = {}
    elif defect == 'control_payload': evidence['control_frames']['WorkerEvent:cut']['business_payload_matches'] = 1
    elif defect == 'control_bound': evidence['control_frames']['WorkerEvent:cut']['maximum_bytes'] = 65537
    elif defect == 'current_main': evidence['current_main_reads'] = ['/main/current.py']
    elif defect == 'recompile': evidence['historical_recompiles'] = 1
    elif defect == 'not_restored': evidence['original_objects_restored'] = False
    with pytest.raises(AssertionError):
        validate_evidence(evidence)


def test_security_delegate_propagates_missing_real_evidence(monkeypatch):
    import runtime_v3_artifact_security_scenarios as scenarios
    import runtime_v3_support as support
    error = support.NotRunError('artifact_security', {'missing':'actual original artifacts'})
    def missing(*args, **kwargs):
        raise error
    monkeypatch.setattr(scenarios, 'artifact_security', missing)
    with pytest.raises(support.NotRunError) as observed:
        support._s_artifact_security(SimpleNamespace(), None)
    assert observed.value is error


def test_security_delegate_does_not_accept_case_names_as_proof(monkeypatch):
    import runtime_v3_artifact_security_scenarios as scenarios
    import runtime_v3_support as support
    monkeypatch.setattr(scenarios, 'artifact_security', lambda *_:{'counterexamples':list(REQUIRED_CASES)})
    with pytest.raises((AssertionError, TypeError, AttributeError)):
        support._s_artifact_security(SimpleNamespace(), None)
