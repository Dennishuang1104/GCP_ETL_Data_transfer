from Environment import cfg
from mysql.MySQL import DBParams, MySqlAdaptor, MySqlDBConnection


class CDP(MySqlAdaptor):
    def __init__(self):
        self.class_name = self.__class__.__name__
        db_params = DBParams(
            host=cfg.get(self.class_name, 'HOST'),
            user=cfg.get(self.class_name, 'USER'),
            password=cfg.get(self.class_name, 'PASSWORD'),
            db=cfg.get(self.class_name, 'DB_NAME'),
            port=int(cfg.get(self.class_name, 'PORT'))
        )
        super(CDP, self).__init__(MySqlDBConnection(db_params))
