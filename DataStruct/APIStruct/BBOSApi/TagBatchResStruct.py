from datetime import timedelta
import dateutil

from DataStruct import BaseStruct


class TagBatchResStruct(BaseStruct):
    def __init__(self, response: dict):
        super(TagBatchResStruct).__init__()
        self._user_id: int = 0 if 'user_id' not in response else response['user_id']
        self._tag_id: int = 0 if 'tag_id' not in response else response['tag_id']
