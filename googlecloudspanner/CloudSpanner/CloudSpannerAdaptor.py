from google.cloud import spanner
from . import CloudSpannerConnection, CloudSpannerParams
from enum import Enum
import functools


class AdaptorMode(Enum):
    DEFAULT = -1
    QUERY_MODE = 0
    INSERT_MODE = 1
    UPDATE_DML_MODE = 2
    UPDATE_MUTATION_MODE = 3
    DELETE_KEYSET_MODE = 4
    DELETE_RANGE_MODE = 5
    UPSERT_MODE = 6
    REPLACE_MODE = 7


def split_list(source_list, wanted_parts=1):
    length = len(source_list)
    return [source_list[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


class CloudSpannerAdaptor:
    def __init__(self, sp_params: CloudSpannerParams):
        self._params = CloudSpannerParams(cert_path=sp_params.cert_path, instance_id=sp_params.instance,
                                          database_id=sp_params.database, project_id=sp_params.project_id)

        self._connection: CloudSpannerConnection = CloudSpannerConnection(self._params)
        self._column_times = 1
        self.mode = AdaptorMode.DEFAULT
        self.__map_mode = {
            AdaptorMode.QUERY_MODE: self.__query_data,
            AdaptorMode.INSERT_MODE: functools.partial(self.__insert_upsert_replace_data, mode='insert'),
            AdaptorMode.UPDATE_DML_MODE: self.__update_data_dml,
            AdaptorMode.UPDATE_MUTATION_MODE: self.__update_data_mutation,
            AdaptorMode.DELETE_KEYSET_MODE: self.__delete_keyset_data,
            AdaptorMode.DELETE_RANGE_MODE: self.__delete_range_data,
            AdaptorMode.UPSERT_MODE: functools.partial(self.__insert_upsert_replace_data, mode='upsert'),
            AdaptorMode.REPLACE_MODE: functools.partial(self.__insert_upsert_replace_data, mode='replace'),
        }

        self.table_name: str = ''
        self.columns: list = []
        self.statement: str = ''
        self.write_data = []
        self.update_data = []
        self.update_params: dict = {}
        self.update_param_types: dict = {}
        self.delete_data = []
        self.delete_range = []

    @property
    def fetch_data(self):
        return self.__fetch_data

    @property
    def QUERY_MODE(self):
        return AdaptorMode.QUERY_MODE

    @property
    def INSERT_MODE(self):
        return AdaptorMode.INSERT_MODE

    @property
    def UPDATE_DML_MODE(self):
        return AdaptorMode.UPDATE_DML_MODE

    @property
    def UPDATE_MUTATION_MODE(self):
        return AdaptorMode.UPDATE_MUTATION_MODE

    @property
    def DELETE_KEYSET_MODE(self):
        return AdaptorMode.DELETE_KEYSET_MODE

    @property
    def DELETE_RANGE_MODE(self):
        return AdaptorMode.DELETE_RANGE_MODE

    @property
    def UPSERT_MODE(self):
        return AdaptorMode.UPSERT_MODE

    @property
    def REPLACE_MODE(self):
        return AdaptorMode.REPLACE_MODE

    def column_times_setter(self, column_times: int):
        self._column_times = column_times

    def exec(self):
        func = self.__map_mode[self.mode]
        func()

    def __query_data(self):
        try:
            with self._connection.database.snapshot() as snapshot:
                results = snapshot.execute_sql(self.statement)
                self.__fetch_data = [row for row in results]
        except Exception as e:
            raise

    def __insert_upsert_replace_data(self, mode: str):
        try:
            extra_columns_count = len(self.columns) + 1
            column_count = len(self.columns) + (extra_columns_count * self._column_times)
            data_count = len(self.write_data)
            mutation_count = column_count * data_count
            total_mutations = 0
            # split data into multiple parts, prevent mutations from exceeding limits
            split_num = int(mutation_count / 20000) + (mutation_count % 20000 > 0)
            split_result = split_list(self.write_data, wanted_parts=split_num)
            for part_num in range(0, len(split_result)):
                self.write_data = split_result[part_num]
                with self._connection.database.batch() as batch:
                    if mode == 'insert':
                        batch.insert(
                            table=self.table_name,
                            columns=self.columns,
                            values=self.write_data,
                        )
                    elif mode == 'upsert':
                        batch.insert_or_update(
                            table=self.table_name,
                            columns=self.columns,
                            values=self.write_data,
                        )
                    elif mode == 'replace':
                        batch.replace(
                            table=self.table_name,
                            columns=self.columns,
                            values=self.write_data,
                        )
                commit_stats = self._connection.database.logger.last_commit_stats
                total_mutations += commit_stats.mutation_count
            print(f'total_mutations: {total_mutations}')
        except Exception as e:
            raise

    def __update_data_dml(self):
        def update(transaction, statement, params, param_types):
            total_rows = 0
            row_ct = transaction.execute_update(
                statement,
                params=params,
                param_types=param_types,
            )
            total_rows += row_ct
            print(f'total_rows: {total_rows}')
        try:
            self._connection.database.run_in_transaction(
                update, self.statement, self.update_params, self.update_param_types
            )
        except Exception as e:
            raise

    def __update_data_mutation(self):
        try:
            extra_columns_count = len(self.columns) + 1
            column_count = len(self.columns) + (extra_columns_count * self._column_times)
            data_count = len(self.update_data)
            mutation_count = column_count * data_count
            total_mutations = 0
            # split data into multiple parts, prevent mutations from exceeding limits
            split_num = int(mutation_count / 20000) + (mutation_count % 20000 > 0)
            split_result = split_list(self.update_data, wanted_parts=split_num)
            for part_num in range(0, len(split_result)):
                self.update_data = split_result[part_num]
                with self._connection.database.batch() as batch:
                    batch.update(
                        table=self.table_name,
                        columns=self.columns,
                        values=self.update_data,
                    )
                commit_stats = self._connection.database.logger.last_commit_stats
                total_mutations += commit_stats.mutation_count
            print(f'total_mutations: {total_mutations}')
        except Exception as e:
            raise

    def __delete_keyset_data(self):
        try:
            extra_columns_count = len(self.columns) + 1
            column_count = len(self.columns) + (extra_columns_count * self._column_times)
            data_count = len(self.delete_data)
            mutation_count = column_count * data_count
            total_mutations = 0
            # split data into multiple parts, prevent mutations from exceeding limits
            split_num = int(mutation_count / 20000) + (mutation_count % 20000 > 0)
            split_result = split_list(self.delete_data, wanted_parts=split_num)
            for part_num in range(0, len(split_result)):
                self.delete_data = split_result[part_num]
                delete = spanner.KeySet(keys=self.delete_data)
                with self._connection.database.batch() as batch:
                    batch.delete(self.table_name, delete)
                commit_stats = self._connection.database.logger.last_commit_stats
                total_mutations += commit_stats.mutation_count
            print(f'total_mutations: {total_mutations}')
        except Exception as e:
            raise

    def __delete_range_data(self):
        try:
            del_range = spanner.KeyRange(start_closed=self.delete_range[0], end_closed=self.delete_range[1])
            delete_set = spanner.KeySet(ranges=[del_range])
            with self._connection.database.batch() as batch:
                batch.delete(self.table_name, delete_set)
            commit_stats = self._connection.database.logger.last_commit_stats
            print(
                "{} mutation(s) in transaction.".format(
                    commit_stats.mutation_count
                )
            )
        except Exception as e:
            raise
