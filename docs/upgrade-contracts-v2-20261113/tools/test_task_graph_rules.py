"""Regression for the Q008 / L017 / L018 design-wiring omission."""
import unittest

from task_graph_rules import wiring_errors


def task(tid, *, new=False, mode='implement', owner='L', callers=()):
    return dict(id=tid, baseline_line=None if new else 1, mode=mode,
                owner=owner, caller_tasks=list(callers))


class WiringTests(unittest.TestCase):
    def test_new_symbol_requires_an_implement_caller(self):
        tasks = [task('Q008', new=True, callers=['L017', 'L018']),
                 task('L017', mode='verify_preserve'), task('L018', mode='verify_preserve')]
        self.assertEqual(len(wiring_errors(tasks)), 3)

    def test_fixing_only_one_route_keeps_the_other_red(self):
        tasks = [task('Q008', new=True, callers=['L017', 'L018']),
                 task('L017'), task('L018', mode='verify_preserve')]
        self.assertEqual(wiring_errors(tasks),
                         ['L018 -> Q008: preserve caller cannot add new wiring'])

    def test_both_mutable_consumers_close_this_class_of_gap(self):
        self.assertEqual(wiring_errors([task('Q008', new=True, callers=['L017', 'L018']),
                                       task('L017'), task('L018')]), [])

    def test_new_orphan_is_rejected(self):
        self.assertTrue(wiring_errors([task('Q008', new=True)]))

    def test_existing_preserved_dependencies_need_no_new_wiring(self):
        self.assertEqual(wiring_errors([task('R023', mode='verify_preserve', callers=['R019']),
                                       task('R019')]), [])

    def test_acceptance_entry_is_invoked_by_its_gate(self):
        self.assertEqual(wiring_errors([task('V012', new=True, owner='V')]), [])


if __name__ == '__main__':
    unittest.main()
