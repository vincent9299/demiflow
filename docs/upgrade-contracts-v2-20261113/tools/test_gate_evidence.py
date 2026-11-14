"""Tests of the acceptance guard, not tests of future DemiFlow features."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from run_gate import ROOT, OUT, evaluate, frozen_mismatches


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.gate={'deselect':[], 'required_nodeids':['test_one'], 'policy':{'min_passed':1}}
        self.report={'pytest_exit_code':0, 'collected':['test_one'], 'tests':{'test_one':[
            {'phase':phase,'outcome':'passed','wasxfail':False} for phase in ['setup','call','teardown']]}}

    def accepted(self, report):
        return evaluate(self.gate,report,0)['status']=='passed'

    def test_complete_pass(self):self.assertTrue(self.accepted(self.report))
    def test_file_silently_missing(self):
        self.gate['selectors']=['tests/absent.py'];self.assertFalse(self.accepted(self.report))
    def test_empty_collection(self):self.report['collected']=[];self.assertFalse(self.accepted(self.report))
    def test_missing_required(self):self.gate['required_nodeids']=['absent'];self.assertFalse(self.accepted(self.report))
    def test_skip(self):self.report['tests']['test_one'][1]['outcome']='skipped';self.assertFalse(self.accepted(self.report))
    def test_xpass(self):self.report['tests']['test_one'][1]['wasxfail']=True;self.assertFalse(self.accepted(self.report))
    def test_failed_teardown(self):self.report['tests']['test_one'][-1]['outcome']='failed';self.assertFalse(self.accepted(self.report))
    def test_incomplete_phases(self):self.report['tests']['test_one'].pop();self.assertFalse(self.accepted(self.report))
    def test_hidden_deselection(self):self.report['deselected']=['test_other'];self.assertFalse(self.accepted(self.report))
    def test_collection_skip(self):self.report['collection_errors']=[{'outcome':'skipped'}];self.assertFalse(self.accepted(self.report))
    def test_process_failure(self):self.assertEqual(evaluate(self.gate,self.report,124)['status'],'not_accepted')
    def test_each_parameter_must_pass(self):
        base=copy.deepcopy(self.report['tests']['test_one'])
        self.report['collected']=['test_one[a]','test_one[b]']
        self.report['tests']={'test_one[a]':base,'test_one[b]':copy.deepcopy(base)}
        self.assertTrue(self.accepted(self.report))
        self.report['tests']['test_one[b]'][1]['outcome']='skipped'
        self.assertFalse(self.accepted(self.report))
    def test_frozen_file_red_control(self):
        import hashlib
        with tempfile.TemporaryDirectory(prefix='demiflow-guard-test-') as tmp:
            root=Path(tmp);(root/'frozen.py').write_bytes(b'original')
            guard={'files':{'frozen.py':hashlib.sha256(b'original').hexdigest()}}
            self.assertFalse(frozen_mismatches(root,guard))
            (root/'frozen.py').write_bytes(b'modified')
            self.assertEqual(frozen_mismatches(root,guard),['frozen.py'])
    def test_real_pytest_plugin_records_skip_and_xfail(self):
        with tempfile.TemporaryDirectory(prefix='demiflow-gate-plugin-test-') as tmp:
            path=Path(tmp);sample=path/'test_sample.py'
            sample.write_text('import pytest\ndef test_pass(): pass\n@pytest.mark.skip(reason="probe")\ndef test_skip(): pass\n@pytest.mark.xfail(reason="probe")\ndef test_xfail(): assert False\n')
            report=path/'evidence.json'
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=str(ROOT)+os.pathsep+str(OUT/'tools'), DEMIFLOW_GATE_ROOT=str(ROOT), DEMIFLOW_GATE_REPORT=str(report))
            env.pop('PYTEST_ADDOPTS',None)
            proc=subprocess.run([sys.executable,'-m','pytest','-q','-p','no:cacheprovider','-p','gate_plugin',str(sample),'--basetemp='+str(path/'pytest-tmp')],cwd=path,env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(proc.returncode,0,proc.stdout+proc.stderr)
            data=json.loads(report.read_text())
            gate={'deselect':[], 'required_nodeids':['test_sample.py::test_pass'], 'policy':{'min_passed':1}}
            self.assertEqual(evaluate(gate,data,proc.returncode)['status'],'not_accepted')
            self.assertEqual(len(data['collected']),3)


if __name__=='__main__':unittest.main()
