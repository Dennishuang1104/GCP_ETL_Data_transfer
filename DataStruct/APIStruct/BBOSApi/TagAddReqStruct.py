from DataStruct import BaseStruct


class TagAddReqStruct(BaseStruct):
    def __init__(self):
        super(TagAddReqStruct).__init__()
        self.user_id: int = 0
