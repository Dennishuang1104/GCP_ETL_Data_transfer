import os
from google.cloud import bigquery
from . import BigQueryParams


class BigQueryConnection:
    def __init__(self, params: BigQueryParams):
        self._params = params
        try:
            self.oriented_google_application_credentials()
            print('create bigquery client...', end='')
            self._client = bigquery.Client()
            print('ok')
        except Exception as e:
            raise

    @property
    def client(self):
        return self._client

    def oriented_google_application_credentials(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._params.cert_path
