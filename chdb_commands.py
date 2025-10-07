import os
import pandas as pd
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from contextlib import contextmanager
from .set_logging import get_logger
from dotenv import load_dotenv
from typing import Optional, List, Dict, Tuple
import inspect
from .utils import *

load_dotenv()

class ClickHouseDatabase:
    def __init__(
        self,
        config: Optional[Dict] = None,
        log_file: str = get_project_dir()+'//logs//clickhouse_db.log',
        terminal = False,
        file_log = True,
        auto_time_process = True
    ):
        self.config = config or self._get_config_from_env()
        self.client = None
        self.auto_time_process = auto_time_process
        self.logger = get_logger(__name__, log_file=log_file, terminal=terminal, file_log=file_log)

    def _get_config_from_env(self) -> Dict:
        """从环境变量获取ClickHouse配置"""
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 9000)),
            "user": os.getenv("DB_USER", "default"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_DATABASE", "default")
        }

    def connect(self) -> Client:
        """建立数据库连接"""
        try:
            self.client = Client(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"]
            )
            self.logger.info(f"Connected to ClickHouse database: {self.config['database']}")
            return self.client
        except ClickHouseError as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise ConnectionError("ClickHouse connection failed") from e

    def close(self):
        """关闭连接（ClickHouse连接池自动管理，通常不需要手动关闭）"""
        if self.client:
            self.client.disconnect()
            self.logger.debug("ClickHouse connection closed")

    @contextmanager
    def cursor(self):
        """提供游标上下文管理器"""
        try:
            if not self.client:
                self.connect()
            yield self.client
        except ClickHouseError as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
        finally:
            pass  # ClickHouse连接自动管理，无需显式关闭

    def execute(self, sql: str):
        """执行SQL语句"""
        with self.cursor() as client:
            try:
                self.logger.debug(f"Executing SQL: {sql}")
                result = client.execute(sql)
                self.logger.info("SQL executed successfully")
                return result
            except ClickHouseError as e:
                self.logger.error(f"SQL Execution failed: {str(e)}")
                raise

    def insert_dataframe(
            self,
            df: pd.DataFrame,
            table_name: str,
            columns: List[str]=None
    ):
        """优化版DataFrame插入"""
        try:
            if self.auto_time_process:
                if 'date' in df.columns:
                    # 转换为日期类型并提取日期部分
                    df['date'] = convert_to_shanghai(pd.to_datetime(df['date'])).dt.date

                if 'date_time' in df.columns:
                    # 转换为带时区的时间戳
                    df['date_time'] = convert_to_shanghai(pd.to_datetime(df['date_time']))

            if columns is None:
                columns = list(df.columns)
            cols = ','.join(columns)
            sql = f"INSERT INTO {table_name} ({cols}) VALUES"
            df = df[columns]
            params = df.to_dict('records')

        
            with self.cursor() as cursor:
                cursor.execute(sql, params)
            self.logger.info(f"Inserted {len(df)} rows into {table_name}")
        except ClickHouseError as e:
            self.logger.error(f"Insert failed: {e.message}")
            raise

    def fetch(self, sql: str, as_df: bool = True) -> pd.DataFrame:
        """执行查询并返回结果"""
        try:
            with self.cursor() as client:
                result, meta = client.execute(sql, with_column_types=True)
                if as_df:
                    columns = [col[0] for col in meta]
                    df = pd.DataFrame(result, columns=columns)
                return df
        except ClickHouseError as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise
