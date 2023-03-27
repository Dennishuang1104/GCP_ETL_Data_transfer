import pandas as pd
import Environment
from bigquery.BigQuery import BigQueryAdaptor, BigQueryConnection, BigQueryParams


class BQ_MASK(BigQueryAdaptor):
    def __init__(self):
        bq_params = BigQueryParams(cert_path=f'{Environment.ROOT_PATH}/cert/gcp-ghr-maskedreader.json')
        self.project_id = 'gcp-20220516-001'
        super().__init__(BigQueryConnection(bq_params))

    def select_data_with(self, statement: str):
        self.statement = statement
        self.mode = self.QUERY_MODE
        self.exec()
        result_df = pd.DataFrame(self.fetch_data)
        return result_df

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