import pandas as pd

import Environment
from bigquery.BigQuery import BigQueryAdaptor, BigQueryConnection, BigQueryParams


class BQ_GH(BigQueryAdaptor):
    def __init__(self):
        bq_params = BigQueryParams(cert_path=f'{Environment.ROOT_PATH}/cert/gcp-ghr-bq.json')
        self.project_id = 'gcp-20190903-01'
        super().__init__(BigQueryConnection(bq_params))

    def insert_data_with(self, dataset_name: str, table_name: str, insert_df: pd.DataFrame()):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.mode = self.INSERT_MODE
        self.insert_data = insert_df
        self.exec()

    def write_truncate_data_with(self, dataset_name: str, table_name: str, insert_df: pd.DataFrame()):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.mode = self.WRITE_TRUNCATE_MODE
        self.insert_data = insert_df
        self.exec()
