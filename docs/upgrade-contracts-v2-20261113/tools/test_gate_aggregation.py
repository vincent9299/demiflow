import copy
import unittest
from aggregate_gates import aggregate


class AggregationTests(unittest.TestCase):
    def setUp(self):
        self.gates={'A':{'depends_on':['B'],'repeat':1},'B':{'depends_on':[],'repeat':3}}
        self.reports=[{'gate':k,'revision':'a'*40,'status':'passed','git_status':'',
                       'frozen_mismatches':[], 'symbol_scope_mismatches':[], 'depends_on':v['depends_on'],
                       'gate_config_sha256':'digest','rounds':[{'status':'passed'}]*v['repeat']}
                      for k,v in self.gates.items()]
    def result(self):return aggregate(self.gates,self.reports,required=['A'],config_sha256='digest')
    def test_complete(self):self.assertEqual(self.result()['status'],'passed');self.assertFalse(self.result()['product_accepted'])
    def test_missing_dependency(self):self.reports.pop();self.assertEqual(self.result()['status'],'not_accepted')
    def test_mixed_revisions(self):self.reports[1]['revision']='b'*40;self.assertEqual(self.result()['status'],'not_accepted')
    def test_missing_repeat(self):self.reports[1]['rounds'].pop();self.assertEqual(self.result()['status'],'not_accepted')
    def test_duplicate(self):self.reports.append(copy.deepcopy(self.reports[0]));self.assertEqual(self.result()['status'],'not_accepted')
    def test_old_config(self):self.reports[1]['gate_config_sha256']='old';self.assertEqual(self.result()['status'],'not_accepted')
    def test_dirty(self):self.reports[1]['git_status']=' M source.py';self.assertEqual(self.result()['status'],'not_accepted')
    def test_dependency_cycle(self):self.gates['B']['depends_on']=['A'];self.assertEqual(self.result()['status'],'not_accepted')


if __name__=='__main__':unittest.main()
