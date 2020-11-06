import sqlite3
from typing import List

from cdc.BaseSourceAdapter import BaseSourceAdapter
from cdc.DataRow import DataRow


class SQLiteRegistryAdapter(BaseSourceAdapter):

    def __init__(self, db_name: str, target_table_name: str, key_columns_name: List[str], data_columns_name: List[str]):
        self.db_name = db_name
        self.target_table_name = target_table_name
        self.keys = key_columns_name
        self.data_cols = data_columns_name

    def get_target_data(self):
        keys = ",".join(self.keys)
        columns = ",".join(self.data_cols)
        conn = sqlite3.connect(self.db_name)
        cursor = conn.execute(f"SELECT {keys},{columns} FROM {self.target_table_name};")

        for row in cursor:
            data_dict = dict(zip(self.keys + self.data_cols, row))
            yield DataRow(data_dict, key_columns=self.keys)

        conn.close()


class SQLiteLogAdapter(BaseSourceAdapter):
    def __init__(self, db_name: str, target_table_name: str, column_names: List[str], timestamp_column_name: str):
        if timestamp_column_name not in column_names:
            column_names.append(timestamp_column_name)

        self.db_name = db_name
        self.target_table_name = target_table_name
        self.column_names = column_names
        self.timestamp_col = timestamp_column_name

    def get_target_data(self, timestamp_delimiter:str):
        columns = ",".join(self.column_names)
        conn = sqlite3.connect(self.db_name)
        cursor = conn.execute(
            f"SELECT {columns} FROM {self.target_table_name} WHERE {self.timestamp_col} > '{timestamp_delimiter}';")

        for row in cursor:
            data_dict = dict(zip(self.column_names, row))
            yield DataRow(data_dict, timestamp_column=self.timestamp_col)

        conn.close()
