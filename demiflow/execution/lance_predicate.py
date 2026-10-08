"""Private scalar equality predicates without Arrow/Substrait schema conversion."""
import math


def scalar_equal(name, value):
    # Lance treats double quotes as string literals; identifiers use backticks.
    identifier = '`' + name.replace('`', '``') + '`'
    if value is None:
        return identifier + ' IS NULL'
    if isinstance(value, str):
        literal = "'" + value.replace("'", "''") + "'"
    elif type(value) is bool:
        literal = 'TRUE' if value else 'FALSE'
    elif type(value) is int:
        literal = str(value)
    elif type(value) is float and math.isfinite(value):
        literal = repr(value)
    else:
        raise ValueError('Lance equality key requires a finite scalar')
    return identifier + ' = ' + literal
