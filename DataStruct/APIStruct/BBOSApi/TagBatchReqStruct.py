from DataStruct import BaseStruct


class TagBatchReqStruct(BaseStruct):
    def __init__(self):
        super(TagBatchReqStruct).__init__()
        self.user_id: list = []
        self.tag_id: int = 0
