"""Normalize jupyter_client's native header dates before bounded JSON counting.

Jupyter wire JSON dates are decoded into datetime by jupyter_client. Keeping
that native object in the JSON-only output accounting would reject every real
message, including status/execute_reply. Only the two protocol header dates
are normalized; arbitrary content/classes are still rejected by contracts.
"""
from datetime import datetime
from demiflow.workbench.analysis import contracts as c


def jupyter_envelope(message):
    if type(message) is not dict:
        c.reject("Jupyter message must be an object")
    if len(message) > 16:
        c.reject("Jupyter envelope has too many fields", code="resource_limit")
    result = dict(message)
    for name in ("header", "parent_header"):
        value = message.get(name)
        if type(value) is not dict or len(value) > 32:
            c.reject("Jupyter message header must be an object")
        date = value.get("date")
        if type(date) is datetime:
            if date.tzinfo is None:
                c.reject("Jupyter header date needs a timezone")
            result[name] = {**value, "date": date.isoformat()}
    buffers = message.get("buffers", [])
    if type(buffers) is not list or buffers:
        c.reject("Binary Jupyter buffers/widgets are unsupported", code="operation_rejected")
    return result
