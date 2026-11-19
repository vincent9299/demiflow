"""Passive, bounded links to exact SDK declarations for source attribution.

References are borrowed only during this compilation. They are not a wire
format, an execution interface, or permission to inspect declaration internals.
"""


def dataset_declaration_links(dataset, *, max_operations=1024):
    if type(max_operations) is not int or not 0 <= max_operations <= 1024:
        raise ValueError('max_operations must be in 0..1024')
    from .dataset import Dataset, MaterializedDataset
    from .plan import LogicalPlan
    if type(dataset) not in (Dataset, MaterializedDataset):
        return None
    values = object.__getattribute__(dataset, '__dict__')
    plan = values.get('_plan')
    if '_source' not in values or type(plan) is not LogicalPlan:
        return None
    operations = plan.operations
    if type(operations) is not tuple or len(operations) > max_operations:
        return None
    return values['_source'], operations
