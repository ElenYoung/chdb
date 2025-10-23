import pandas as pd
import numpy as np
import os
import inspect


def convert_to_nullable_object(series: pd.Series) -> pd.Series:
    """
    将 Nullable 类型列中的 [np.nan, pd.NaT, pd.NA] 转为 None，并转为 object 类型
    """
    if series.isna().any():
        series = series.astype(object).where(series.notna(), None)
    return series


def convert_to_shanghai(series):
    """将时间序列转换为上海时区，自动处理已有/缺失时区的情况"""
    if series.dt.tz is None:
        return series.dt.tz_localize('Asia/Shanghai')
    else:
        return series.dt.tz_convert('Asia/Shanghai')
    
def get_project_dir():
    """获取调用者项目的目录，兼容.py文件和.ipynb文件"""
    # 获取调用栈，索引1是调用当前库的文件
    frame = inspect.stack()[1]
    caller_file = frame[1]
    
    # 检查是否是Jupyter Notebook环境（文件名以<ipython-input-开头）
    if caller_file.startswith('<ipython-input-'):
        # Jupyter Notebook中，使用当前工作目录
        return os.getcwd()
    else:
        # 标准Python文件，使用调用文件的目录
        return os.path.dirname(os.path.abspath(caller_file))