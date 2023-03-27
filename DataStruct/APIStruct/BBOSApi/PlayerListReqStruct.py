from DataStruct import BaseStruct


class PlayerListReqStruct(BaseStruct):
    def __init__(self):
        super(PlayerListReqStruct).__init__()
        self.tag_ids: int = 0
        self.first_results: int = 0
        self.max_results: int = 100
