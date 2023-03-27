from DataStruct import BaseStruct


class IconResStruct(BaseStruct):
    def __init__(self, response: dict):
        super(IconResStruct).__init__()
        self._id: int = 0 if 'id' not in response else response['id']
        self._name: str = '' if 'name' not in response else response['name']
        self._used: bool = False if 'used' not in response else response['used']
