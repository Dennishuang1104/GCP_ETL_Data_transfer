from datetime import timedelta
import dateutil

from DataStruct import BaseStruct


class TagUserCountResStruct(BaseStruct):
    def __init__(self, response: dict):
        super(TagUserCountResStruct).__init__()

        self._id: int = 0 if 'id' not in response else response['id']
        self._user_count: int = 0 if 'user_count' not in response else response['user_count']
