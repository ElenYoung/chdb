# quantchdb: A Well-Encapsulated ClickHouse Database APIs Lib

## Quick Start

Install chdb:

```
pip install quantchdb==0.1.6  -i https://pypi.org/simple
```

An example of how to use chdb:

```python

from quantchdb import ClickHouseDatabase

# To connect your clickhouse database, you need to setup your config, in which the '.env' method is recommmended for security
config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 9000)),
            "user": os.getenv("DB_USER", "default"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_DATABASE", "default")
        }

# 'terminal' and 'file_log' control the log records. 'True' denotes the corresponding log method will be executed. You can control the logs' file path by  the 'log_file' param. 
db = ClickHouseDatabase(config=config, terminal=True, file_log=False)

# Fetch data from clickhouse database
sql = "SELECT * FROM stocks.snap ORDER BY date DESC LIMIT 5"
df = db.fetch(sql)

# Execute sql commands
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
    `volume` Float32 NULL COMMENT 'K线内成交量',
    `amount` Float32 NULL COMMENT 'K线内成交额'
)Engine = ReplacingMergeTree()
ORDER BY (code, date_time);
"""
db.execute(sql)

# Insert dataframe into clickhouse database. Before you insert your dataframe, you need to make sure the corresponding database and table are existed.

db.insert_dataframe(
            df=df,
            table_name="etf.kline_1m",
            columns=["exg", "code", "date_time", "date", "time_int", "open", "high", "low", "close", "volume", "amount"]
        )
```
