"""Analysis helpers for the DataFrame already selected in a notebook."""
__demiflow_data_tools__ = {"column_profile": "analysis"}


def column_profile(frame):
    """查看当前批次各列的类型、非空数和缺失数。"""
    import pandas as pd

    present = frame.count()
    return pd.DataFrame({"dtype": frame.dtypes.astype(str), "present": present,
                         "missing": len(frame) - present})
