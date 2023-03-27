import logging
import os

from . import CloudSpannerParams
from google.cloud import spanner


class CloudSpannerConnection:
    def __init__(self, params: CloudSpannerParams):
        self._params = params
        try:
            class CommitStatsLogger(logging.Logger):
                def __init__(self):
                    self.last_commit_stats = None
                    super().__init__("commit_stats_sample")

                def info(self, msg, *args, **kwargs):
                    if kwargs["extra"] and "commit_stats" in kwargs["extra"]:
                        self.last_commit_stats = kwargs["extra"]["commit_stats"]
                    super().info(msg)

            self.oriented_google_application_credentials()
            print('create spanner client...', end='')
            self._client = spanner.Client(project=self._params.project_id)
            self._instance = self._client.instance(self._params.instance)
            self._database = self._instance.database(self._params.database, logger=CommitStatsLogger())
            self._database.log_commit_stats = True

            print('ok')
        except Exception as e:
            raise

    @property
    def client(self):
        return self._client

    @property
    def instance(self):
        return self._instance

    @property
    def database(self):
        return self._database

    def oriented_google_application_credentials(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._params.cert_path
