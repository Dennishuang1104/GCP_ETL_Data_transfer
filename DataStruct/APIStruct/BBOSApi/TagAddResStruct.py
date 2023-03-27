from datetime import timedelta
import dateutil

from DataStruct import BaseStruct


class TagAddResStruct(BaseStruct):
    def __init__(self, response: dict):
        super(TagAddResStruct).__init__()
        self._domain: int = 0 if 'domain' not in response else response['domain']
        self._icon: str = '' if 'icon' not in response else response['icon']
        self._icon_id: int = 0 if 'icon_id' not in response else response['icon_id']
        self._id: int = 0 if 'id' not in response else response['id']
        self._name: str = '' if 'name' not in response else response['name']
        self._removed: bool = False if 'removed' not in response else response['removed']
        self._role: int = 0 if 'role' not in response else response['role']
