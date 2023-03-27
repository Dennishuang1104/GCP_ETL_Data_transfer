from CloudSpanner.CloudSpannerAdaptor import CloudSpannerAdaptor, CloudSpannerConnection, CloudSpannerParams


class ExampleClass(CloudSpannerAdaptor):
    def __init__(self, database: str):
        self.insert_data = []
        sp_params = CloudSpannerParams(cert_path='cert/gcp-ghr-spanner.json',
                                       instance_id='cdp-instance', database_id=database, 
                                       project_id='gcp-20190903-01')
        super().__init__(sp_params)

    def insert_data_with(self, table_name: str):
        self.table_name = table_name
        self.mode = self.INSERT_MODE
        self.exec()
