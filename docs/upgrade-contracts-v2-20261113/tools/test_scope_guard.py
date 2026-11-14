import unittest
from scope_guard import outside_scope_changed


class ScopeTests(unittest.TestCase):
    def setUp(self):
        self.source='import math\nVALUE=1\nclass Stage:\n    __slots__=("x",)\n    def run(self): return 1\n    def protected(self): return 2\n'
        self.allowed={'Stage.run','Stage.__slots__'}
    def changed(self, source):return outside_scope_changed(self.source,source,self.allowed)
    def test_allowed_body(self):self.assertFalse(self.changed(self.source.replace('return 1','return 5')))
    def test_allowed_slots(self):self.assertFalse(self.changed(self.source.replace('("x",)','("x","adapter")')))
    def test_sibling_blocked(self):self.assertTrue(self.changed(self.source.replace('return 2','return 9')))
    def test_import_blocked(self):self.assertTrue(self.changed(self.source.replace('import math','import os')))
    def test_global_blocked(self):self.assertTrue(self.changed(self.source.replace('VALUE=1','VALUE=2')))
    def test_rename_blocked(self):self.assertTrue(self.changed(self.source.replace('def run(','def different(')))
    def test_duplicate_blocked(self):self.assertTrue(self.changed(self.source+'    def run(self): return 3\n'))
    def test_comment_allowed(self):self.assertFalse(self.changed('# comment\n'+self.source))


if __name__=='__main__':unittest.main()
