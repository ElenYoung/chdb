import pandas as pd
import numpy as np
import os
import inspect


def convert_to_nullable_object(series: pd.Series) -> pd.Series:
    """
    Convert Nullable types [np.nan, pd.NaT, pd.NA] to None, alse object type
    """
    if series.isna().any():
        series = series.astype(object).where(series.notna(), None)
    return series


def convert_to_shanghai(series):
    """Convert timezone to Asia/Shanghai"""
    if series.dt.tz is None:
        return series.dt.tz_localize('Asia/Shanghai')
    else:
        return series.dt.tz_convert('Asia/Shanghai')
    
def get_project_dir():
    """get project dir, compatible with.py and .ipynb"""
    # get call stack, index 1 is current file
    frame = inspect.stack()[1]
    caller_file = frame[1]
    
    if caller_file.startswith('<ipython-input-'):
        return os.getcwd()
    else:
        return os.path.dirname(os.path.abspath(caller_file))