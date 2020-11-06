import hashlib
import json

from typing import List


class DataRow(dict):

    def __init__(self, data_dict: dict, key_columns: List[str] = None, timestamp_column: str = None):
        self.data = data_dict

        if key_columns:
            # TODO: fix hashing of nested dictionaries.
            #   This is possibly the most inefficient way of doing that
            key_values = {k: v for k, v in self.data.items() if k in key_columns}
            data_values = {k: v for k, v in self.data.items() if k not in key_columns}

            self.key_hash = hashlib.sha256(json.dumps(key_values, sort_keys=True).encode('utf-8')).hexdigest()
            self.hash = hashlib.sha256(json.dumps(data_values, sort_keys=True).encode('utf-8')).hexdigest()

            dict.__init__(self, key_hash=self.key_hash, hash=self.hash, data=self.data)
        elif timestamp_column:
            self.timestamp_str = self.data[timestamp_column]
            del self.data[timestamp_column]

            dict.__init__(self, timestamp=self.timestamp_str, data=data_dict)
        else:
            raise ValueError("Define key_columns or timestamp_column based on the chosen CDC strategy")
