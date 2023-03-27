import pandas as pd
import Environment
from googlecloudspanner.CloudSpanner import CloudSpannerAdaptor, CloudSpannerConnection, CloudSpannerParams


class CDP_BBOS(CloudSpannerAdaptor):
    def __init__(self, database: str):
        sp_params = CloudSpannerParams(
            cert_path=f'{Environment.ROOT_PATH}/cert/gcp-ghr-spanner.json',
            instance_id='cdp-instance',
            database_id=database
        )
        self.insert_data = []
        super().__init__(sp_params)

    def select_data_with(self, columns: list, statement: str):
        self.statement = statement
        self.mode = self.QUERY_MODE
        self.exec()
        result_df = pd.DataFrame(self.fetch_data, columns=columns)
        return result_df

    def insert_data_with(self, table_name: str, audit_data: pd.DataFrame()):
        self.columns = audit_data.columns.to_list()
        self.write_data = audit_data.values.tolist()
        self.table_name = table_name
        self.mode = self.INSERT_MODE
        self.exec()

    def upsert_data_with(self, table_name: str, audit_data: pd.DataFrame()):
        self.columns = audit_data.columns.to_list()
        self.write_data = audit_data.values.tolist()
        self.table_name = table_name
        self.mode = self.UPSERT_MODE
        self.exec()

    def replace_data_with(self, table_name: str, audit_data: pd.DataFrame()):
        self.columns = audit_data.columns.to_list()
        self.write_data = audit_data.values.tolist()
        self.table_name = table_name
        self.mode = self.REPLACE_MODE
        self.exec()

    def update_data_with(self, table_name: str, audit_data: pd.DataFrame()):
        self.columns = audit_data.columns.to_list()
        self.update_data = audit_data.values.tolist()
        self.table_name = table_name
        self.mode = self.UPDATE_MUTATION_MODE
        self.exec()

    def update_data_dml_with(self, statement: str):
        self.statement = statement
        self.mode = self.UPDATE_DML_MODE
        self.exec()

    def delete_keyset_data_with(self, table_name: str, audit_data: pd.DataFrame()):
        self.delete_data = audit_data.values.tolist()
        self.columns = audit_data.columns
        self.table_name = table_name
        self.mode = self.DELETE_KEYSET_MODE
        self.exec()

