import os

from . import Params
from google.cloud import storage


class CloudStorageConnection:
    def __init__(self, params: Params):
        self._params = params
        self.oriented_google_application_credentials()
        print('create storage client...', end='')
        self._client = storage.Client().from_service_account_json(self._params.cret_path)
        print('ok')

    @property
    def client(self):
        return self._client

    def oriented_google_application_credentials(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._params.cret_path

