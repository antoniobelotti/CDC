import json
import os
import random
from datetime import datetime

from cdc.BaseDataLakeAdapter import BaseDataLakeAdapter


class DummyDataLakeAdapter(BaseDataLakeAdapter):
    """
        Create a fake directory tree in CWD and read/write only json files

        DELETE previous data on __init__
    """

    def __init__(self, app_name, process_id):
        super(DummyDataLakeAdapter, self).__init__(app_name, process_id)
        self.BASE_PATH = os.path.join(os.getcwd(), self.BASE_PATH)

        if not os.path.exists(self.BASE_PATH):
            os.makedirs(self.BASE_PATH)

        for file in os.listdir(self.BASE_PATH):
            os.remove(os.path.join(self.BASE_PATH, file))

        with open(os.path.join(self.BASE_PATH, "sync.json"), "w") as empty_sync_file:
            json.dump({}, empty_sync_file)

    def get_sync_data(self):
        with open(os.path.join(self.BASE_PATH, "sync.json"), "r") as sync_file_text:
            result = json.load(sync_file_text)
        return result

    def write_sync_data(self, data, temp=False):
        extension = "tmp" if temp else "json"
        with open(os.path.join(self.BASE_PATH, f"sync.{extension}"), "w") as sync_file_text:
            json.dump(data, sync_file_text)
            sync_file_text.flush()

    def write_capture_data(self, data, temp=False):
        extension = "tmp" if temp else "json"

        # add random prefix to the file because running this on simple examples makes more files have the same
        # timestamp because it's to fast (and the files are overwritten)
        prefix = random.randint(1,999)
        file_path = os.path.join(self.BASE_PATH, f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{extension}")
        with open(file_path, "w") as incr_change_file:
            json.dump(data, incr_change_file)
            incr_change_file.flush()

    def commit_tmp_files(self):
        for file_name in os.listdir(self.BASE_PATH):
            if file_name.split(".")[-1] == "tmp":
                src = os.path.join(self.BASE_PATH, file_name)
                dest = os.path.join(self.BASE_PATH, file_name.replace(".tmp", ".json"))

                try:
                    os.rename(src, dest)
                except FileExistsError:
                    os.remove(dest)
                    os.rename(src, dest)

    def purge_uncommitted(self):
        for file_name in os.listdir(self.BASE_PATH):
            if file_name.split(".")[-1] == "tmp":
                os.remove(os.path.join(self.BASE_PATH, file_name))
