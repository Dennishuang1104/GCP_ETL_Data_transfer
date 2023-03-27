class CloudSpannerParams:
    def __init__(self, cert_path: str, instance_id: str, database_id: str, project_id=None):
        self._project_id: str = 'gcp-20190903-01' if project_id is None else project_id
        self._cert_path: str = cert_path
        self._instance_id: str = instance_id
        self._database_id: str = database_id

    @property
    def project_id(self):
        return self._project_id

    @property
    def cert_path(self):
        return self._cert_path

    @property
    def instance(self):
        return self._instance_id

    @property
    def database(self):
        return self._database_id
