import os
import atexit
import weakref
import pandas as pd
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from contextlib import contextmanager
from .set_logging import get_logger
from dotenv import load_dotenv
from typing import Optional, List, Dict, Annotated
from .utils import *
from pandas.api.types import (
    is_integer_dtype, is_float_dtype, is_bool_dtype, 
    is_datetime64_any_dtype, is_object_dtype, is_categorical_dtype,
    is_string_dtype
)
from datetime import date, time

load_dotenv()

# 全局实例跟踪器，用于程序退出时清理所有连接
_active_instances = weakref.WeakSet()


def _cleanup_all_instances():
    """程序退出时清理所有活跃的数据库连接"""
    for instance in list(_active_instances):
        try:
            instance.close()
        except Exception:
            pass


atexit.register(_cleanup_all_instances)

class ClickHouseDatabase:
    """
    ClickHouse 数据库连接管理类。
    
    支持以下使用方式:
    
    1. 上下文管理器（推荐）:
        with ClickHouseDatabase(config=config) as db:
            df = db.fetch("SELECT * FROM table")
        # 自动关闭连接
    
    2. 手动管理:
        db = ClickHouseDatabase(config=config)
        try:
            df = db.fetch("SELECT * FROM table")
        finally:
            db.close()  # 必须手动关闭！
    
    警告: 不使用上下文管理器时，必须显式调用 close() 方法，
    否则可能导致套接字泄漏！
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        log_file: str = None,
        terminal_log: bool = False,
        file_log: bool = False,
        auto_time_process: bool = True
    ):
        """
        初始化 ClickHouseDatabase 实例。
        
        Args:
            config: 数据库配置字典，包含 host, port, user, password, database
            log_file: 日志文件路径，默认为项目目录下的 logs/clickhouse_db.log
            terminal_log: 是否输出到终端
            file_log: 是否写入日志文件
            auto_time_process: 是否自动处理时间列
        """
        if log_file is None:
            log_file = os.path.join(get_project_dir(), 'logs', 'clickhouse_db.log')
        
        self.config = config or self._get_config_from_env()
        self.client: Optional[Client] = None
        self.auto_time_process = auto_time_process
        self._closed = False
        self.logger = get_logger(
            __name__, 
            log_file=log_file, 
            terminal_log=terminal_log, 
            file_log=file_log
        )
        
        # 注册到全局跟踪器
        _active_instances.add(self)

    def __enter__(self) -> 'ClickHouseDatabase':
        """进入上下文管理器，建立连接"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """退出上下文管理器，关闭连接"""
        self.close()
        return None

    def __del__(self):
        """析构函数，确保连接被关闭"""
        if not self._closed:
            try:
                self.close()
            except Exception:
                pass

    def _get_config_from_env(self) -> Dict:
        """
        从环境变量获取配置。
        
        Returns:
            包含数据库连接配置的字典
        """
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 9000)),
            "user": os.getenv("DB_USER", "default"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_DATABASE", "default")
        }

    @property
    def is_connected(self) -> bool:
        """检查是否已建立连接"""
        return self.client is not None and not self._closed

    def connect(self) -> Client:
        """
        建立数据库连接。
        
        如果已有连接则直接返回，避免重复创建。
        
        Returns:
            ClickHouse Client 实例
            
        Raises:
            ConnectionError: 连接失败时抛出
        """
        if self._closed:
            raise RuntimeError("Cannot connect: database instance has been closed")
        
        if self.client is not None:
            return self.client
        
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

    def close(self) -> None:
        """
        关闭数据库连接。
        
        可以安全地多次调用此方法。
        """
        if self._closed:
            return
        
        if self.client is not None:
            try:
                self.client.disconnect()
                self.logger.debug("ClickHouse connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
            finally:
                self.client = None
        
        self._closed = True

    def reconnect(self) -> Client:
        """
        重新建立数据库连接。
        
        Returns:
            新的 ClickHouse Client 实例
        """
        if self.client is not None:
            try:
                self.client.disconnect()
            except Exception:
                pass
            self.client = None
        
        self._closed = False
        return self.connect()

    @contextmanager
    def cursor(self):
        """
        提供数据库游标的上下文管理器。
        
        会自动建立连接（如果尚未连接）。
        
        Yields:
            ClickHouse Client 实例
            
        Raises:
            ClickHouseError: 查询执行失败时抛出
        """
        if self._closed:
            raise RuntimeError("Cannot get cursor: database instance has been closed")
        
        try:
            if self.client is None:
                self.connect()
            yield self.client
        except ClickHouseError as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    def execute(self, sql: str) -> List:
        """
        执行 SQL 语句。
        
        Args:
            sql: 要执行的 SQL 语句
            
        Returns:
            执行结果列表
            
        Raises:
            ClickHouseError: SQL 执行失败时抛出
            
        Example:
            >>> db.execute("CREATE DATABASE IF NOT EXISTS test")
            >>> db.execute("DROP TABLE IF EXISTS test.my_table")
        """
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
            columns: Optional[List[str]] = None,
            datetime_cols: Optional[List[str]] = None,
            convert_tz: bool = True
    ) -> None:
        """
        将 DataFrame 插入到数据库表中。
        
        Args:
            df: 要插入的 DataFrame
            table_name: 目标表名（格式：database.table 或 table）
            columns: 要插入的列名列表，默认为 DataFrame 的所有列
            datetime_cols: 需要进行时间类型转换的列名列表
            convert_tz: 是否将时区转换为 Asia/Shanghai，默认为 True
            
        Raises:
            ClickHouseError: 插入失败时抛出
            
        Note:
            - 此方法不会修改原始 DataFrame
            - 如果 auto_time_process=True 且指定了 datetime_cols，会自动转换时间格式
            - NULL 值会自动转换为 None 以兼容 ClickHouse
            
        Example:
            >>> db.insert_dataframe(
            ...     df=my_df,
            ...     table_name="stocks.daily",
            ...     datetime_cols=['trade_date'],
            ...     convert_tz=True
            ... )
        """
        try:
            # 创建副本，避免修改原始 DataFrame
            df_copy = df.copy()
            
            if self.auto_time_process and datetime_cols:
                for datetime_col in datetime_cols:
                    if datetime_col in df_copy.columns:
                        df_copy[datetime_col] = pd.to_datetime(df_copy[datetime_col])
                        if convert_tz:
                            df_copy[datetime_col] = convert_to_shanghai(df_copy[datetime_col])

            # Convert null type to None, which is acceptable in ClickHouse
            na_ser = df_copy.isna().any()
            na_cols = list(na_ser[na_ser].index)
            if na_cols:
                df_copy[na_cols] = df_copy[na_cols].apply(convert_to_nullable_object, axis=0)

            if columns is None:
                columns = list(df_copy.columns)
            
            cols = ','.join(columns)
            sql = f"INSERT INTO {table_name} ({cols}) VALUES"
            df_insert = df_copy[columns]
            params = df_insert.to_dict('records')

            with self.cursor() as cursor:
                cursor.execute(sql, params)
            self.logger.info(f"Inserted {len(df_insert)} rows into {table_name}")
        except ClickHouseError as e:
            self.logger.error(f"Insert failed: {e.message}")
            raise
    
    def fetch(
            self, 
            sql: str, 
            as_df: bool = True
    ):
        """
        执行查询并返回结果。
        
        Args:
            sql: 要执行的 SELECT 查询语句
            as_df: 是否以 DataFrame 格式返回，默认为 True
            
        Returns:
            如果 as_df=True，返回 pd.DataFrame
            如果 as_df=False，返回 tuple(result, meta)，其中 meta 包含列类型信息
            
        Raises:
            ClickHouseError: 查询失败时抛出
            
        Example:
            >>> df = db.fetch("SELECT * FROM stocks.daily LIMIT 10")
            >>> result, meta = db.fetch("SELECT * FROM stocks.daily", as_df=False)
        """
        try:
            with self.cursor() as client:
                result, meta = client.execute(sql, with_column_types=True)
                if as_df:
                    columns = [col[0] for col in meta]
                    return pd.DataFrame(result, columns=columns)
                else:
                    return (result, meta)
        except ClickHouseError as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise
    
    def create_table_from_df(
            self,
            df: pd.DataFrame,
            table_name: str,
            dtypes: Optional[Dict[str, str]] = None,
            engine: str = 'MergeTree()',
            orderby: str = 'tuple()',
            other: Optional[str] = None
    ) -> None:
        """
        根据 DataFrame 创建表并插入数据。
        
        Args:
            df: 用于推断表结构的 DataFrame
            table_name: 表名（格式：database.table 或 table）
            dtypes: 指定列的 ClickHouse 类型，如 {'col1': 'Int64', 'date': 'Date'}
            engine: 表引擎，默认为 'MergeTree()'
            orderby: ORDER BY 子句，默认为 'tuple()'
            other: 其他 DDL 子句，如 'PARTITION BY toYYYYMM(date)'
            
        Raises:
            ClickHouseError: 创建表或插入数据失败时抛出
            
        Warning:
            自动类型推断可能不够准确，建议明确指定 dtypes 参数。
            
        Example:
            >>> db.create_table_from_df(
            ...     df=my_df,
            ...     table_name='test.my_table',
            ...     dtypes={'code': 'String', 'date': 'Date'},
            ...     engine='ReplacingMergeTree()',
            ...     orderby='(code, date)'
            ... )
        """
        # 修复可变默认参数问题
        if dtypes is None:
            dtypes = {}
            
        try:
            columns_with_types = self.infer_clickhouse_schema(df)
            datetime_cols = []
            
            for col, dtype in dtypes.items():
                columns_with_types[col] = dtype
                if 'Date' in dtype:
                    datetime_cols.append(col)

            columns_def = ', '.join([f"`{col}` {dtype}" for col, dtype in columns_with_types.items()])
            
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def}) ENGINE = {engine}"
            if orderby:
                sql += f" ORDER BY {orderby}"
            if other:
                sql += f" {other}"

            # 执行建表和插入（复用同一连接）
            with self.cursor() as cursor:
                cursor.execute(sql)
                self.logger.info(f"Table {table_name} created successfully")
            
            # 插入数据
            self.insert_dataframe(df=df, table_name=table_name, datetime_cols=datetime_cols if datetime_cols else None)
            
        except ClickHouseError as e:
            self.logger.error(f"Create table failed: {e.message}")
            raise


    def infer_clickhouse_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        将 pandas DataFrame 的数据类型映射到 ClickHouse 数据类型。
        
        Args:
            df: 要推断类型的 DataFrame
            
        Returns:
            列名到 ClickHouse 类型的映射字典
            
        Note:
            类型映射规则：
            - int -> Int8/16/32/64 或 UInt8/16/32/64（根据值范围）
            - float32 -> Float32, float64 -> Float64
            - bool -> UInt8
            - datetime -> Date 或 DateTime（根据是否有时间部分）
            - object/string -> String
            - 包含 NULL 值的列会被包装为 Nullable(...)
        """
        schema = {}
        
        for col in df.columns:
            dtype = df[col].dtype
            col_series = df[col]
            
            # check null（NaN/NaT/pd.NA）
            has_missing = col_series.isna().any()
            
            # Int
            if is_integer_dtype(dtype):
                min_val = col_series.min()
                max_val = col_series.max()
                
                if min_val >= 0:  
                    if max_val <= 255:
                        base_type = "UInt8"
                    elif max_val <= 65535:
                        base_type = "UInt16"
                    elif max_val <= 4294967295:
                        base_type = "UInt32"
                    else:
                        base_type = "UInt64"
                else:  
                    if min_val >= -128 and max_val <= 127:
                        base_type = "Int8"
                    elif min_val >= -32768 and max_val <= 32767:
                        base_type = "Int16"
                    elif min_val >= -2147483648 and max_val <= 2147483647:
                        base_type = "Int32"
                    else:
                        base_type = "Int64"
                
                schema[col] = f"Nullable({base_type})" if has_missing else base_type
            
            # 2. Float
            elif is_float_dtype(dtype):
                if col_series.dtype == 'float32':
                    base_type = "Float32"
                else:
                    base_type = "Float64"
                schema[col] = f"Nullable({base_type})" if has_missing else base_type
            
            # 3. Bool
            elif is_bool_dtype(dtype):
                schema[col] = f"Nullable(UInt8)" if has_missing else "UInt8"
            
            # 4. DateTime
            elif is_datetime64_any_dtype(dtype):
                
                if all(ts.time() == time(0, 0) for ts in col_series if not pd.isna(ts)):
                    base_type = "Date"
                else:
                    base_type = "DateTime"
                schema[col] = f"Nullable({base_type})" if has_missing else base_type
            
            # 5. String
            elif is_object_dtype(dtype) or is_string_dtype(dtype) or is_categorical_dtype(dtype):
                if all(isinstance(x, date) for x in col_series if not pd.isna(x)):
                    base_type = "Date"
                    schema[col] = f"Nullable({base_type})" if has_missing else base_type
                else:
                    schema[col] = f"Nullable(String)" if has_missing else "String"
            
            # 6. Default
            else:
                schema[col] = f"Nullable(String)" if has_missing else "String"
        
        return schema