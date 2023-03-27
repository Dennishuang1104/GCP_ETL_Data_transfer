from googlecloudstorage.CloudStorage import CloudStorageAdaptor


class GCS(CloudStorageAdaptor):
    def __init__(self, bucket_name: str, cert_path: str):
        super(GCS, self).__init__(bucket_name, cert_path)
