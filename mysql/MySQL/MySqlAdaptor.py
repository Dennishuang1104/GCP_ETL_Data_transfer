from . import MySqlDBConnection
from enum import Enum


class AdaptorMode(Enum):
    DEFAULT = -1
    QUERY_MODE = 0
    INSERT_MODE = 1
    UPDATE_MODE = 2
    DELETE_MODE = 3


class MySqlAdaptor:
    def __init__(self, DBConnection:MySqlDBConnection):
        self.db_connection = DBConnection
        self.statement = ''
        self.write_data = []
        self.write_rowcount = -1
        self.query_conditions = ()
        self.update_data = []
        self.__map_mode = {
            AdaptorMode.QUERY_MODE: self.__get_data,  # get_data, query mode
            AdaptorMode.INSERT_MODE: self.__insert_data,  # insert mode
            AdaptorMode.UPDATE_MODE: self.__update_data,  # update mode
            AdaptorMode.DELETE_MODE: self.__delete_data  # delete mode
        }
        self.mode = AdaptorMode.DEFAULT
        self.__fetch_data = []

        self.__transaction_connection = None

    @property
    def QUERY_MODE(self):
        return AdaptorMode.QUERY_MODE

    @property
    def INSERT_MODE(self):
        return AdaptorMode.INSERT_MODE

    @property
    def UPDATE_MODE(self):
        return AdaptorMode.UPDATE_MODE

    @property
    def DELETE_MODE(self):
        return AdaptorMode.DELETE_MODE

    @property
    def fetch_data(self):
        return self.__fetch_data

    def exec(self):
        func = self.__map_mode[self.mode]
        func()

    def __get_data(self):
        connection = self.db_connection.get_connection_from_pool() if self.__transaction_connection is None else self.__transaction_connection
        cursor = connection.cursor()
        self.__fetch_data = []
        try:
            if len(self.query_conditions) > 0:
                cursor.executemany(self.statement, self.query_conditions)
            else:
                cursor.execute(self.statement)

            self.__fetch_data = cursor.fetchall()

            if self.__transaction_connection is None:
                connection.commit()

        except:
            raise

        finally:
            if self.__transaction_connection is not None:
                return

            if connection is not None:
                self.db_connection.put_connection_into_pool(connection)

    def __insert_data(self):
        connection = self.db_connection.get_connection_from_pool() if self.__transaction_connection is None else self.__transaction_connection
        cursor = connection.cursor()
        try:
            cursor.executemany(self.statement, self.write_data)
            self.write_rowcount = cursor.rowcount
            if self.__transaction_connection is None:
                connection.commit()
        except Exception as e:
            connection.rollback()
            raise
        finally:
            if self.__transaction_connection is not None:
                return

            if connection is not None:
                self.db_connection.put_connection_into_pool(connection)

    def __update_data(self):
        connection = self.db_connection.get_connection_from_pool() if self.__transaction_connection is None else self.__transaction_connection
        cursor = connection.cursor()
        try:
            cursor.executemany(self.statement, self.update_data)
            self.write_rowcount = cursor.rowcount
            if self.__transaction_connection is None:
                connection.commit()
        except Exception as e:
            connection.rollback()
            raise
        finally:
            if self.__transaction_connection is not None:
                return

            if connection is not None:
                self.db_connection.put_connection_into_pool(connection)

    def __delete_data(self):
        connection = self.db_connection.get_connection_from_pool() if self.__transaction_connection is None else self.__transaction_connection
        cursor = connection.cursor()
        try:
            cursor.execute(self.statement)
            self.write_rowcount = cursor.rowcount
            if self.__transaction_connection is None:
                connection.commit()

        except Exception as e:
            connection.rollback()

        finally:
            if self.__transaction_connection is not None:
                return

            if connection is not None:
                self.db_connection.put_connection_into_pool(connection)

    def begin_transaction(self):
        """
        啟用 transaction 模式
        :return:
        """
        if self.__transaction_connection is not None:
            self.db_connection.put_connection_into_pool(self.__transaction_connection)

        self.__transaction_connection = self.db_connection.get_connection_from_pool()

    def end_transaction(self):
        """
        關閉 transaction 模式
        :return:
        """
        if self.__transaction_connection is not None:
            try:
                self.__transaction_connection.commit()
            except:
                self.__transaction_connection.rollback()
            finally:
                if self.__transaction_connection is not None:
                    self.db_connection.put_connection_into_pool(self.__transaction_connection)
        self.__transaction_connection = None
