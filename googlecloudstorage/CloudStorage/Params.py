class Params:
    def __init__(self, cret_path: str, bucket_name: str):
        self._cret_path: str = cret_path
        self._bucket_name: str = bucket_name

    @property
    def cret_path(self):
        return self._cret_path

    @property
    def bucket(self):
        return self._bucket_name

