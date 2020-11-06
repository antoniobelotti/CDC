import os
from abc import ABC, abstractmethod


class BaseDataLakeAdapter(ABC):

    def __init__(self, app_name, process_id):
        self.BASE_PATH = os.path.join("datalake","cdc", app_name, process_id)

    @abstractmethod
    def write_capture_data(self, data, temp=False):
        raise NotImplementedError()

    @abstractmethod
    def write_sync_data(self, data, temp=False):
        raise NotImplementedError()

    @abstractmethod
    def get_sync_data(self):
        raise NotImplementedError()

    @abstractmethod
    def commit_tmp_files(self):
        raise NotImplementedError()

    @abstractmethod
    def purge_uncommitted(self):
        raise NotImplementedError()