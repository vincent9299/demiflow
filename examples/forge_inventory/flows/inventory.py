"""Two fixed inputs, native grouping, explicit scope guard and two outputs."""
from demiflow import data
from operators.inventory import count_state, reconcile, shipped_quantity, unique_order


def run(*, orders, shipments, detail, summary, tolerance=0):
    data.require(tolerance >= 0, 'tolerance must be nonnegative')
    expected = data.read_lance(orders['uri'], version=orders['version']).reduce_by_key('order_id', unique_order).materialize()
    shipped = data.read_lance(shipments['uri'], version=shipments['version']).reduce_by_key('order_id', shipped_quantity).materialize()
    data.require(shipped.join(expected, on='order_id', how='anti').count() == 0, 'Shipment refers to an unknown order')
    result = expected.join(shipped, on='order_id', how='left').map(reconcile, fn_kwargs={'tolerance': tolerance}).materialize()
    receipt = result.write_lance(detail['uri'], mode='create', return_receipt=True)
    result.reduce_by_key('status', count_state).write_lance(summary['uri'], mode='create', return_receipt=True)
    return receipt
