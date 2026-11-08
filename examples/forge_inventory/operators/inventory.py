"""Order/shipment reconciliation policy for an independent non-model Flow."""
from typing import NotRequired, TypedDict


class Order(TypedDict):
    order_id: str | None
    expected: int | None


class Shipment(TypedDict):
    order_id: str | None
    quantity: int | None


class Shipped(TypedDict):
    order_id: str
    shipped: int


class Joined(TypedDict):
    order_id: str | None
    expected: int | None
    shipped: NotRequired[int | None]


class Reconciled(TypedDict):
    order_id: str
    expected: int
    shipped: int
    difference: int
    status: str


class Count(TypedDict):
    status: str
    count: int


def unique_order(acc: Order | None, row: Order) -> Order:
    if acc is not None:
        raise ValueError('Order ID is duplicated')
    key = row['order_id']
    expected = row['expected']
    if key is None or not key or expected is None or expected < 0:
        raise ValueError('Order requires a nonempty ID and nonnegative expected quantity')
    return row


def shipped_quantity(acc: Shipped | None, row: Shipment) -> Shipped:
    key = row['order_id']
    quantity = row['quantity']
    if key is None or not key or quantity is None or quantity < 0:
        raise ValueError('Shipment requires an ID and nonnegative quantity')
    return {'order_id': key, 'shipped': (acc['shipped'] if acc is not None else 0) + quantity}


def reconcile(row: Joined, tolerance: int) -> Reconciled:
    key = row['order_id']
    expected = row['expected']
    if key is None or expected is None or tolerance < 0:
        raise ValueError('Invalid order or tolerance')
    shipped = row.get('shipped') or 0
    difference = shipped - expected
    return {'order_id': key, 'expected': expected, 'shipped': shipped, 'difference': difference,
            'status': 'matched' if abs(difference) <= tolerance else 'short' if difference < 0 else 'excess'}


def count_state(acc: Count | None, row: Reconciled) -> Count:
    return {'status': row['status'], 'count': (acc['count'] if acc is not None else 0) + 1}
