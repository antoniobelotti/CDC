from datetime import datetime

from cdc import BaseSourceAdapter
from cdc.BaseDataLakeAdapter import BaseDataLakeAdapter


class CDC:
    def __init__(self, source: BaseSourceAdapter, data_lake: BaseDataLakeAdapter, strategy: str = "log",
                 transactional: bool = False, datetime_format: str = None):
        if strategy != "log" and strategy != "registry":
            raise ValueError("Strategy must be either 'log' or 'registry'")
        if strategy == "log" and datetime_format is None:
            raise ValueError("Specify a datetime format to use log strategy")

        self.source = source
        self.data_lake = data_lake
        self.transactional = transactional
        self.strategy = strategy
        self.datetime_format = datetime_format

    def run(self):
        if self.transactional:
            self.data_lake.purge_uncommitted()

        if self.strategy == "log":
            self.__run_log_cdc()
        else:
            self.__run_registry_cdc()

        if self.transactional:
            self.data_lake.commit_tmp_files()

    def __run_registry_cdc(self):
        old_sync_data = self.data_lake.get_sync_data()
        candidate_fresh_rows = self.source.get_target_data()

        new_sync_data = {}
        for fresh_row in candidate_fresh_rows:
            old_row_hash = old_sync_data.get(fresh_row.key_hash, None)
            if old_row_hash is None or old_row_hash != fresh_row.hash:
                self.data_lake.write_capture_data(fresh_row, self.transactional)
                new_sync_data[fresh_row.key_hash] = fresh_row.hash
            elif old_row_hash:
                # if the data hash exists and it's the same, leave the corresponding sync.json line untouched
                new_sync_data[fresh_row.key_hash] = old_row_hash

        self.data_lake.write_sync_data(new_sync_data, self.transactional)

    def __run_log_cdc(self):
        old_sync_data = self.data_lake.get_sync_data()

        # get min(sync.json timestamp , '1970-01-01 01:00:00')
        ts_query_delimiter_str = old_sync_data.get("timestamp", datetime.fromtimestamp(0).strftime(self.datetime_format))

        # max_ts will contain the maximum timestamp between all the timestamp of the fresh rows
        max_ts = datetime.strptime(ts_query_delimiter_str, self.datetime_format)

        for fresh_row in self.source.get_target_data(ts_query_delimiter_str):
            self.data_lake.write_capture_data(fresh_row, self.transactional)

            # convert fresh row timestamp to datetime so it's comparable with max_ts
            fresh_row_datetime_timestamp = datetime.strptime(fresh_row.timestamp_str, self.datetime_format)

            max_ts = max_ts if max_ts > fresh_row_datetime_timestamp else fresh_row_datetime_timestamp

        self.data_lake.write_sync_data({"timestamp": str(max_ts)}, self.transactional)
