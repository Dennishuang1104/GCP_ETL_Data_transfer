import pymysql
from pymysql import Connection
import queue
from . import DBParams


def check_connection_alive(connection: Connection):
    alive = True
    try:
        connection.ping(reconnect=True)
    except:
        alive = False
    finally:
        return alive


class MySqlDBConnection:
    def __init__(self, params: DBParams):
        self.params = params
        self._connection_pool = queue.Queue(5)
        self.build_connection_into_pool()

    def create_a_new_connection(self):
        try:
            new_connection = pymysql.connect(
                host=self.params.host,
                port=self.params.port,
                user=self.params.user,
                password=self.params.password,
                db=self.params.db,
                charset=self.params.charset,
                unix_socket=self.params.instance
            )

            return new_connection
        except:
            raise

    def put_connection_into_pool(self, connection):
        try:
            self._connection_pool.put(connection)
        except:
            raise

    def build_connection_into_pool(self, multiple_conn_num=1):
        try:
            for _ in range(multiple_conn_num):
                self._connection_pool.put(self.create_a_new_connection())
        except:
            raise

    def get_connection_from_pool(self):
        try:
            connection = self._connection_pool.get()
            if check_connection_alive(connection) is False:
                connection = self.create_a_new_connection()
            return connection
        except:
            raise
