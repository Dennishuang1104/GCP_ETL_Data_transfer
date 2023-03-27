from . import CloudStorageConnection, Params
from enum import Enum
import time

from typing import Dict


class AdaptorMode(Enum):
    DEFAULT = -1
    DOWNLOAD_MODE = 0
    UPLOAD_MODE = 1
    BATCH_DOWNLOAD_MODE = 2
    BATCH_UPLOAD_MODE = 3
    CHECK_EXIST_MODE = 4

class CloudStorageAdaptor:
    def __init__(self, bucket_name: str, cret_path: str):
        self._params = Params(cret_path=cret_path, bucket_name=bucket_name)
        self._connection: CloudStorageConnection = CloudStorageConnection(self._params)
        self.mode = AdaptorMode.DEFAULT
        self.__map_mode = {
            AdaptorMode.DOWNLOAD_MODE: self.__download_by,  # download
            AdaptorMode.UPLOAD_MODE: self.__upload_with,  # upload
            AdaptorMode.BATCH_DOWNLOAD_MODE: self.__batch_download_by,  # batch download
            AdaptorMode.BATCH_UPLOAD_MODE: self.__batch_upload_with,  # batch upload
            AdaptorMode.CHECK_EXIST_MODE: self.__check_file_exist,  # check file exist
        }
        self.blob: str = ''
        self.file: str = ''
        self.batch_config: Dict = {}

    @property
    def DOWNLOAD_MODE(self):
        return AdaptorMode.DOWNLOAD_MODE

    @property
    def UPLOAD_MODE(self):
        return AdaptorMode.UPLOAD_MODE

    @property
    def BATCH_DOWNLOAD_MODE(self):
        return AdaptorMode.BATCH_DOWNLOAD_MODE

    @property
    def BATCH_UPLOAD_MODE(self):
        return AdaptorMode.BATCH_UPLOAD_MODE

    @property
    def CHECK_EXIST_MODE(self):
        return AdaptorMode.CHECK_EXIST_MODE

    def exec(self):
        func = self.__map_mode[self.mode]
        self._connection.oriented_google_application_credentials()
        return func()

    def __download_by(self):
        try:
            print(f'get bucket {self._params.bucket}...', end='')
            st = time.time()
            bucket = self._connection.client.get_bucket(self._params.bucket)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            print(f'blob {self.blob} download to {self.file}... ', end='')
            st = time.time()
            blob = bucket.blob(self.blob)
            blob.download_to_filename(self.file)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')
            pass
        except:
            raise

    def __batch_download_by(self):
        try:
            print(f'get bucket {self._params.bucket}...', end='')
            st = time.time()
            bucket = self._connection.client.get_bucket(self._params.bucket)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            for source_blob_name, destination_file_name in self.batch_config.items():
                print(f'blob {source_blob_name} download to {destination_file_name}... ', end='')
                st = time.time()
                blob = bucket.blob(source_blob_name)
                blob.download_to_filename(destination_file_name)
                et = time.time()
                print(f'ok, cost time {round(et - st, 3)} second')
        except:
            raise

    def __upload_with(self):
        try:
            print(f'get bucket {self._params.bucket}...', end='')
            st = time.time()
            bucket = self._connection.client.get_bucket(self._params.bucket)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            print(f'blob {self.file} upload to {self.blob}... ', end='')
            st = time.time()
            blob = bucket.blob(self.blob)
            blob.upload_from_filename(self.file)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

        except:
            raise

    def __batch_upload_with(self):
        try:
            print(f'get bucket {self._params.bucket}...', end='')
            st = time.time()
            bucket = self._connection.client.get_bucket(self._params.bucket)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            for destination_blob_name, source_file_name in self.batch_config.items():
                print(f'blob {source_file_name} upload to {destination_blob_name}... ', end='')
                st = time.time()
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(source_file_name)
                et = time.time()
                print(f'ok, cost time {round(et - st, 3)} second')
        except:
            raise

    def __check_file_exist(self):
        try:
            print(f'get bucket {self._params.bucket}...', end='')
            st = time.time()
            bucket = self._connection.client.get_bucket(self._params.bucket)
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            print(f'blob {self.file} upload to {self.blob}... ', end='')
            st = time.time()
            blob = bucket.blob(self.blob)
            result = blob.exists()
            et = time.time()
            print(f'ok, cost time {round(et - st, 3)} second')

            return result
        except:
            raise