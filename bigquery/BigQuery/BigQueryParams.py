class BigQueryParams:
    def __init__(self, cert_path: str):
        self._cert_path = cert_path

    @property
    def cert_path(self):
        return self._cert_path
