"""
blob: the filename in google cloud storage (can be a folder with name ex: imgs/xxx.jpg)
file: local file (can be a path with filename)
"""
from googlecloudstorage.CloudStorage.Adaptor import CloudStorageAdaptor
import Environment as envi


class GCSAdaptor(CloudStorageAdaptor):
    def __init__(self):
        super(GCSAdaptor, self).__init__('ssr_etl_log', envi.VTC_SSR_BQ_CERT)
        self.logs_folder = envi.logs_folder

    def mkdir(self, Date):
        self.mode = self.UPLOAD_MODE
        self.blob = Date
        self.file = envi.logs_folder + Date + "/"
        self.exec().__create_folder()

    def upload(self, logs_name):
        self.mode = self.UPLOAD_MODE
        self.blob = logs_name
        self.file = self.logs_folder + logs_name
        self.exec()

    def download(self, logs_name):
        self.mode = self.DOWNLOAD_MODE
        self.blob = logs_name
        self.file = self.logs_folder + logs_name
        self.exec()