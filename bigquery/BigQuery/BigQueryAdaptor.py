import pandas as pd
from google.cloud import bigquery
from .BigQueryConnection import BigQueryConnection
from enum import Enum


class AdaptorMode(Enum):
    DEFAULT = -1
    QUERY_MODE = 0
    PAGE_MODE = 1
    INSERT_MODE = 2
    WRITE_TRUNCATE_MODE = 3
    INSERT_ROW_MODE = 4


class BigQueryAdaptor:
    def __init__(self, connection: BigQueryConnection):
        self.__connection = connection.client
        self.statement = ''
        self.query_conditions = ()
        self.__map_mode = {
            AdaptorMode.QUERY_MODE: self.__get_data,
            AdaptorMode.PAGE_MODE: self.__get_data_pagination,
            AdaptorMode.INSERT_MODE: self.__insert_data,
            AdaptorMode.WRITE_TRUNCATE_MODE: self.___write_truncate_data,
            AdaptorMode.INSERT_ROW_MODE: self.__insert_row_from_dataframe
        }
        self.mode = AdaptorMode.DEFAULT
        self.insert_data: pd.DataFrame = None
        self.__fetch_data = pd.DataFrame()
        self.dataset_name: str = None
        self.table_name: str = None
        self.page_row_num: int = 1000

    @property
    def QUERY_MODE(self):
        return AdaptorMode.QUERY_MODE

    @property
    def PAGE_MODE(self):
        return AdaptorMode.PAGE_MODE

    @property
    def INSERT_MODE(self):
        return AdaptorMode.INSERT_MODE

    @property
    def WRITE_TRUNCATE_MODE(self):
        return AdaptorMode.WRITE_TRUNCATE_MODE

    @property
    def INSERT_ROW_MODE(self):
        return AdaptorMode.INSERT_ROW_MODE

    @property
    def fetch_data(self):
        return self.__fetch_data

    def exec(self):
        func = self.__map_mode[self.mode]
        func()

    def __get_data(self):
        self.__fetch_data = []
        try:
            query_job = self.__connection.query(self.statement)
            self.__fetch_data = query_job.to_dataframe()
        except Exception as e:
            raise

    def __get_data_pagination(self):
        self.__fetch_data = []
        try:
            query_job = self.__connection.query(self.statement)
            query_job.result()  # Wait for the query to complete.

            destination = query_job.destination
            destination = self.__connection.get_table(destination)

            # Download rows.
            #
            # The client library automatically handles pagination.
            rows_iter = self.__connection.list_rows(destination, max_results=self.page_row_num)
            print(f'query total rows: {rows_iter.total_rows}')
            result_df = rows_iter.to_dataframe()

            while rows_iter.next_page_token:
                rows_iter = self.__connection.list_rows(destination,
                                                        max_results=self.page_row_num,
                                                        page_token=rows_iter.next_page_token)
                rows_df = rows_iter.to_dataframe()
                result_df = pd.concat([result_df, rows_df])
            result_df.reset_index(drop=True, inplace=True)
            print(f'result df rows: {len(result_df)}')
            self.__fetch_data = result_df
        except Exception as e:
            raise

    def __insert_data(self):
        self.__fetch_data = []
        try:
            if self.dataset_name is not None and self.table_name is not None:
                dataset_ref = self.__connection.dataset(self.dataset_name)
                table_ref = dataset_ref.table(self.table_name)
                job = self.__connection.load_table_from_dataframe(self.insert_data, table_ref)
                job.result()  # Waits for table load to complete.
                assert job.state == "DONE"
        except Exception as e:
            raise

    def ___write_truncate_data(self):
        self.__fetch_data = []
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            if self.dataset_name is not None and self.table_name is not None:
                dataset_ref = self.__connection.dataset(self.dataset_name)
                table_ref = dataset_ref.table(self.table_name)
                job = self.__connection.load_table_from_dataframe(self.insert_data, table_ref, job_config=job_config)
                job.result()  # Waits for table load to complete.
                assert job.state == "DONE"
        except Exception as e:
            raise

    def __insert_row_from_dataframe(self):
        self.__fetch_data = []
        try:
            if self.dataset_name is not None and self.table_name is not None:
                table_ref = self.__connection.get_table(self.dataset_name + '.' + self.table_name)
                job = self.__connection.insert_rows_from_dataframe(table_ref, self.insert_data)
                if len(job[0]) > 0:
                    raise ValueError(job)
        except Exception as e:
            raise
