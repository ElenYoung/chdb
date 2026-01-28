# quantchdb: A Well-Encapsulated ClickHouse Database APIs Lib

## Quick Start

Install quantchdb:

```
pip install quantchdb==0.1.11  -i https://pypi.org/simple
```

An example of how to use quantchdb:

## 1. Import quantchdb
```python
from quantchdb import ClickHouseDatabase
import pandas as pd
import numpy as np
import os 
from dotenv import load_dotenv

load_dotenv()
```


## 2. Configure ClickHouseDatabase instance

```python
# To connect your clickhouse database, you need to setup your config, in which the '.env' method is recommmended for security
config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 9000)),
            "user": os.getenv("DB_USER", "default"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_DATABASE", "default")
        }
```

### ✅ 推荐用法：使用上下文管理器（Context Manager）

**这是最安全的用法，连接会在退出 `with` 块时自动关闭：**

```python
# 使用 with 语句，连接会自动关闭，避免资源泄漏
with ClickHouseDatabase(config=config, terminal_log=True, file_log=False) as db:
    df = db.fetch("SELECT * FROM your_table LIMIT 10")
    # 在 with 块内进行所有数据库操作
    db.insert_dataframe(df, "another_table")
# 退出 with 块后，连接自动关闭
```

### ⚠️ 手动管理连接（需要显式关闭）

如果不使用上下文管理器，**必须** 手动调用 `close()` 方法：

```python
db = ClickHouseDatabase(config=config, terminal_log=True, file_log=False)
try:
    df = db.fetch("SELECT * FROM your_table LIMIT 10")
finally:
    db.close()  # 必须调用！否则会导致套接字泄漏
```

### 🚨 危险用法警告

以下用法可能导致 **套接字/连接泄漏**，请务必避免：

```python
# ❌ 危险：在循环中创建多个实例而不关闭
for i in range(1000):
    db = ClickHouseDatabase(config=config)
    df = db.fetch(f"SELECT * FROM table_{i}")
    # 没有调用 db.close()，套接字泄漏！

# ❌ 危险：在函数中创建实例但不关闭
def get_data():
    db = ClickHouseDatabase(config=config)
    return db.fetch("SELECT * FROM table")  # db 没有被关闭！

# ❌ 危险：作为类属性但不在 __del__ 中清理
class MyClass:
    def __init__(self):
        self.db = ClickHouseDatabase(config=config)  # 可能泄漏
```

**正确的做法：**

```python
# ✅ 正确：循环中使用上下文管理器
for i in range(1000):
    with ClickHouseDatabase(config=config) as db:
        df = db.fetch(f"SELECT * FROM table_{i}")

# ✅ 正确：复用单个实例
db = ClickHouseDatabase(config=config)
try:
    for i in range(1000):
        df = db.fetch(f"SELECT * FROM table_{i}")
finally:
    db.close()

# ✅ 正确：函数内使用上下文管理器
def get_data():
    with ClickHouseDatabase(config=config) as db:
        return db.fetch("SELECT * FROM table")

# ✅ 正确：类中正确管理连接生命周期
class MyClass:
    def __init__(self):
        self.db = ClickHouseDatabase(config=config)
    
    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.db.close()
```


## 3. Functions

**注意：以下示例假设使用上下文管理器或已正确管理连接生命周期。**

```python
with ClickHouseDatabase(config=config, terminal_log=True) as db:
    # Fetch data from clickhouse database
    sql = "SELECT * FROM stocks.snap ORDER BY date DESC LIMIT 5"
    df = db.fetch(sql)

    # Execute SQL sentence
    sql = f"""
    CREATE TABLE IF NOT EXISTS etf.kline_1m(
        `exg` UInt8 NOT NULL COMMENT '交易所标识，沪市为1，深市为0， 北交所为2',
        `code` String NOT NULL COMMENT '股票代码',
        `date` Date NOT NULL COMMENT '日期',
        `date_time` DateTime('Asia/Shanghai') NOT NULL COMMENT '日期时间，最高精度为秒',
        `time_int` UInt32 NOT NULL COMMENT '从当日开始至当前时刻的毫秒数',
        `open` Float32 NULL COMMENT 'K线开始价格',
        `high` Float32 NULL COMMENT 'K线内最高价',
        `low` Float32 NULL COMMENT 'K线内最低价',
        `close` Float32 NULL COMMENT 'K线结束价格',
        `volume` UInt64 NULL COMMENT 'K线内成交量',
        `amount` Float32 NULL COMMENT 'K线内成交额'
    )Engine = ReplacingMergeTree()
    ORDER BY (code, date_time);
    """
    db.execute(sql)

    # Insert dataframe into clickhouse database. 
    # Before you insert your dataframe, you need to make sure the corresponding database and table are existed.
    # Make sure the dtypes of DataFrame is consistent with dtypes of clickhouse table, or else insert_dataframe may failed.
    # Note: insert_dataframe() will NOT modify the original DataFrame.

    file_path = "Your/Data/Path/kline_1m.csv"
    dtype_dict = {
        'exg' : int,
        'code' : str,
        'open' : np.float32,
        'close' : np.float32,
        'high' : np.float32,
        'low' : np.float32,
        'amount' : np.float32
    }
    df = pd.read_csv(file_path, dtype=dtype_dict)

    # Int type with NA need to deal with seperately
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('UInt64')

    # convert_tz defaults to True, will auto-convert timezone to Asia/Shanghai
    db.insert_dataframe(
                df=df,
                table_name="etf.kline_1m",
                datetime_cols=['date','date_time']
                # convert_tz=True is the default
            )

    # Create table from DataFrame and insert data into table automatically. 
    # This method is not recommanded, because data type inferred may be not suitable or even the sentence failed.
    # You can use dtypes to make sure some columns have corrected dtypes and use other params to control the create sql sentence.

    db.create_table_from_df(df=df, 
                            table_name='test.etf_kline_1m',
                            dtypes={'code': 'String',
                                'date':'Date',
                                'date_time' :'DateTime'},
                         engine='ReplacingMergeTree()', 
                         orderby='(code,date_time)',
                         other='PARTITION BY toYYYYMM(code)')
```
## 4. API Reference

### 属性

| 属性 | 类型 | 描述 |
|------|------|------|
| `is_connected` | `bool` | 检查是否已建立连接 |
| `config` | `Dict` | 数据库配置 |
| `client` | `Client` | ClickHouse 客户端实例 |

### 方法

| 方法 | 描述 |
|------|------|
| `connect()` | 建立数据库连接 |
| `close()` | 关闭数据库连接（可安全多次调用） |
| `reconnect()` | 重新建立连接 |
| `execute(sql)` | 执行 SQL 语句 |
| `fetch(sql, as_df=True)` | 查询数据并返回 DataFrame 或原始结果 |
| `insert_dataframe(df, table_name, ...)` | 将 DataFrame 插入表中 |
| `create_table_from_df(df, table_name, ...)` | 根据 DataFrame 创建表并插入数据 |

## 5. Best Practices (最佳实践)

### 连接管理

1. **优先使用上下文管理器** (`with` 语句)，它能确保连接在任何情况下都被正确关闭
2. **复用连接**：如果需要执行多个操作，在同一个 `with` 块内完成
3. **避免在循环中创建新实例**：这会导致套接字快速耗尽

### 性能优化

1. **批量插入**：使用 `insert_dataframe()` 一次性插入大量数据
2. **合理设置日志级别**：生产环境建议关闭 `terminal_log` 和 `file_log`

### 错误处理

```python
from clickhouse_driver.errors import Error as ClickHouseError

with ClickHouseDatabase(config=config) as db:
    try:
        df = db.fetch("SELECT * FROM non_existent_table")
    except ClickHouseError as e:
        print(f"Database error: {e}")
    except ConnectionError as e:
        print(f"Connection error: {e}")
```

## 6. Changelog

### v0.2.0 (Breaking Changes)
- ✅ 添加上下文管理器支持（`with` 语句）
- ✅ 添加 `is_connected` 属性
- ✅ 添加 `reconnect()` 方法
- ✅ 添加程序退出时自动清理所有连接的机制
- ✅ 改进错误处理和日志记录
- ✅ 修复套接字泄漏问题