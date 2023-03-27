class DBParams:
    def __init__(self, host, user, password, db, port=3306, charset='utf8', instance=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._charset = charset
        self._db = db
        self._instance = instance
        self.connection_pool = []

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def db(self):
        return self._db

    @property
    def charset(self):
        return self._charset

    @property
    def instance(self):
        return self._instance



