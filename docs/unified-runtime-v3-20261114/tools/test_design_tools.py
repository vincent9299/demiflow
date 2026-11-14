import copy, unittest
from evidence_guard import evaluate

class GuardTests(unittest.TestCase):
    def base(self):
        return dict(exit_code=0,collected=['tests/a.py::test_a'],collection_errors=[],deselected=[],source_before={'a':'1'},source_after={'a':'1'},module_paths={'demiflow':'/tmp/tree/demiflow/__init__.py'},results=[dict(nodeid='tests/a.py::test_a',when=x,outcome='passed') for x in ['setup','call','teardown']])
    def check(self,r):return evaluate(r,required_nodeids=['tests/a.py::test_a'],expected_tree='/tmp/tree',expected_digests={'a':'1'})['passed']
    def test_complete(self):self.assertTrue(self.check(self.base()))
    def test_zero(self):r=self.base();r['collected']=[];self.assertFalse(self.check(r))
    def test_missing(self):r=self.base();r['collected']=['tests/b.py::test_b'];self.assertFalse(self.check(r))
    def test_duplicate(self):r=self.base();r['results'].append(r['results'][1]);self.assertFalse(self.check(r))
    def test_retry(self):r=self.base();r['results'][1]['rerun']=1;self.assertFalse(self.check(r))
    def test_fail_then_pass(self):r=self.base();r['results'].append(dict(r['results'][1],outcome='failed'));self.assertFalse(self.check(r))
    def test_skip(self):r=self.base();r['results'][1]['outcome']='skipped';self.assertFalse(self.check(r))
    def test_xfail(self):r=self.base();r['results'][1]['wasxfail']='bug';self.assertFalse(self.check(r))
    def test_setup_missing(self):r=self.base();r['results']=r['results'][1:];self.assertFalse(self.check(r))
    def test_wrong_tree(self):r=self.base();r['module_paths']['demiflow']='/main/demiflow/__init__.py';self.assertFalse(self.check(r))
    def test_submodule_wrong_tree(self):r=self.base();r['module_paths']['demiflow.a']='/main/demiflow/a.py';self.assertFalse(self.check(r))
    def test_source_drift(self):r=self.base();r['source_after']={'a':'2'};self.assertFalse(self.check(r))
    def test_collection_error(self):r=self.base();r['collection_errors']=['bad'];self.assertFalse(self.check(r))
    def test_deselect(self):r=self.base();r['deselected']=['other'];self.assertFalse(self.check(r))
    def test_nonzero(self):r=self.base();r['exit_code']=1;self.assertFalse(self.check(r))
    def test_teardown_failed(self):r=self.base();r['results'][-1]['outcome']='failed';self.assertFalse(self.check(r))
if __name__=='__main__':unittest.main()

from design_rules import manifest_errors
class ManifestTests(unittest.TestCase):
    def base(self):
        return [dict(id='A001',file='a.py',symbol='entry',lane='A',mode='modify',baseline={'sha256':'x'},calls=['B001'],callers=[],behavior='a',counterexample='a',test_nodeid='test_a'),dict(id='B001',file='b.py',symbol='new',lane='B',mode='add',baseline=None,calls=[],callers=['A001'],behavior='b',counterexample='b',test_nodeid='test_b')]
    def test_valid(self):self.assertEqual(manifest_errors(self.base()),[])
    def test_missing_callee(self):x=self.base();x[0]['calls']=['Z'];self.assertIn('missing_callee',manifest_errors(x))
    def test_missing_caller(self):x=self.base();x[1]['callers']=['Z'];self.assertIn('missing_caller',manifest_errors(x))
    def test_preserve_caller(self):x=self.base();x[0]['mode']='verify_preserve';self.assertIn('unauthorized_caller',manifest_errors(x))
    def test_retired_caller(self):x=self.base();x[0]['mode']='retire';self.assertIn('unauthorized_caller',manifest_errors(x))
    def test_orphan(self):x=self.base();x[1]['callers']=[];self.assertIn('orphan_add',manifest_errors(x))
    def test_owner_overlap(self):x=self.base();x[1]['file']='a.py';self.assertIn('multiple_file_owners',manifest_errors(x))
    def test_duplicate_id(self):x=self.base();x.append(copy.deepcopy(x[0]));self.assertIn('duplicate_id',manifest_errors(x))
    def test_missing_test(self):x=self.base();x[0]['test_nodeid']=None;self.assertIn('missing_acceptance',manifest_errors(x))
    def test_asymmetry(self):x=self.base();x[0]['calls']=[];self.assertIn('asymmetric_edge',manifest_errors(x))
    def test_missing_baseline(self):x=self.base();x[0]['baseline']=None;self.assertIn('unfounded_modification',manifest_errors(x))
