from DataStruct import BaseStruct


class TagUserResetReqStruct(BaseStruct):
    def __init__(self):
        super(TagUserResetReqStruct).__init__()
        self.id: int = 0
