import os
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

class ClickHouseDatabase:
    def __init__(
        self,
        config: Optional[Dict] = None,
        log_file: str = get_project_dir()+'//logs//clickhouse_db.log',
        terminal_log = False,
        file_log = False,
        auto_time_process = True
    ):
        self.config = config or self._get_config_from_env()
        self.client = None
        self.auto_time_process = auto_time_process
        self.logger = get_logger(__name__, log_file=log_file, terminal_log=terminal_log, file_log=file_log)

    def _get_config_from_env(self) -> Dict:
        """
        Get configs from .env
        """
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 9000)),
            "user": os.getenv("DB_USER", "default"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_DATABASE", "default")
        }

    def connect(self) -> Client:
        """build connection"""
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
        """close connection (clickhouse manages connection automatically)"""
        if self.client:
            self.client.disconnect()
            self.logger.debug("ClickHouse connection closed")

    @contextmanager
    def cursor(self):
        """
        provide cursor
        """
        try:
            if not self.client:
                self.connect()
            yield self.client
        except ClickHouseError as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
        finally:
            pass  # ClickHouse manages connections automatically

    def execute(self, sql: str):
        """Execute SQL sentence"""
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
            columns: Annotated[list,"the columns you want to insert into table"]=None,
            datetime_cols: Annotated[list,"the list of columns with datetime type"]=None,
            convert_tz: Annotated[bool, "True denotes convert the timezone to Asia/Shanghai"]=True
    ):
        """
        Insert DataFrame into database.table.
        """
        try:
            if self.auto_time_process:
                if datetime_cols:
                    for datetime_col in datetime_cols:
                        if datetime_col in df.columns:
                            df[datetime_col] = pd.to_datetime(df[datetime_col])
                            if convert_tz:
                                df[datetime_col] = convert_to_shanghai(df[datetime_col])

            #Convert null type to None, which is acceptable in clickhouse for int/string/bool types
            na_ser = df.isna().any()
            na_cols = list(na_ser[na_ser == True].index)
            df[na_cols] = df[na_cols].apply(convert_to_nullable_object, axis=0)

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
        """fetch data based on sql sentence"""
        try:
            with self.cursor() as client:
                result, meta = client.execute(sql, with_column_types=True)
                if as_df:
                    columns = [col[0] for col in meta]
                    res = pd.DataFrame(result, columns=columns)
                else:
                    res = (result, meta)
                return res
        except ClickHouseError as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise
    
    def create_table_from_df(self,
            df: pd.DataFrame,
            table_name: str,
            dtypes: Annotated[dict, "the specific dtypes you want to set, such as {'col1':'Int64'}. types are automatically inferred by default"]={},
            engine: Annotated[str, "the Engine of table, 'MergeTree' by default"]='MergeTree()',
            orderby: Annotated[str, "ORDER BY sentence of CREATE sentence, 'tuple()' by default"] = 'tuple()',
            other: Annotated[str, "other suffix sentence you want to add, such as 'PARTITION BY toYear(TradingDate)'"]=None
        ):
        """Create table based on DataFrame"""
        try:
            columns_with_types = self.infer_clickhouse_schema(df)
            datetime_cols = []
            for col, dtype in dtypes.items():
                columns_with_types[col] = dtype
                if 'Date' in dtype:
                    datetime_cols.append(col)

            columns_def = ', '.join([f"{col} {dtype}" for col, dtype in columns_with_types.items()])
            
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def}) ENGINE = {engine}"
            if orderby:
                sql += f" ORDER BY {orderby}"
            if other:
                sql += f" {other}"

            with self.cursor() as cursor:
                cursor.execute(sql)
            
            with self.cursor() as cursor:
                self.insert_dataframe(df=df, table_name=table_name, datetime_cols=datetime_cols)

            self.logger.info(f"Table {table_name} created successfully")
        except ClickHouseError as e:
            self.logger.error(f"Create table failed: {e.message}")
            raise


    def infer_clickhouse_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        mapping dtypes of pandas DataFrame to ClickHouse dtypes
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